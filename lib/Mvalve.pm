# $Id$

package Mvalve;
use Moose;
use Moose::Util::TypeConstraints;
use Mvalve::Message;
use Mvalve::Throttler;
use Time::HiRes();

our $VERSION   = '0.00007';
our $AUTHORITY = "cpan:DMAKI";

role_type 'Mvalve::Queue';
role_type 'Mvalve::State';

{
    my $coerce = sub {
        my $default_class = shift;
        my $prefix = shift;
        return sub {
            my $h = $_;
            my $module = delete $h->{module} || $default_class;
            if ($prefix && $module !~ s/^\+//) {
                $module = join('::', $prefix, $module);
            }
            Class::MOP::load_class($module);

            $module->new(%{$h->{args}});
        };
    };

    coerce 'Mvalve::Throttler'
        => from 'HashRef'
        => $coerce->('Data::Valve', 'Mvalve::Throttler'), # XXX - no need to use via () here
    ;

    coerce 'Mvalve::Queue'
        => from 'HashRef'
        => $coerce->('Q4M', 'Mvalve::Queue'), # XXX - no need to use via () here
    ;
}

coerce 'Mvalve::State'
    => from 'HashRef'
        => via {
            my $h = $_;
            my $module = delete $h->{module} || 'Memory';
            if ($module !~ s/^\+//) {
                $module = "Mvalve::State::$module";
            }
            Class::MOP::load_class($module);

            $module->new(%{$h->{args}});
        }
;

has 'throttler' => (
    is       => 'rw',
    does     => 'Mvalve::Throttler',
    required => 1,
    coerce   => 1,
    handles  => [ qw(try_push) ],
);

{
    my $default = sub {
        my $class = shift;
        return sub {
            Class::MOP::load_class($class);
            $class->new;
        };
    };

    has 'queue_set' => (
        is  => 'ro',
        isa => 'Mvalve::QueueSet',
        default => $default->( 'Mvalve::QueueSet' )
    );

    has 'state' => (
        is => 'rw',
        does => 'Mvalve::State',
        coerce => 1,
        required => 1,
        default => $default->( 'Mvalve::State::Memory' ),
        handles => {
            map { ("state_$_" => $_) } qw(get set remove incr decr)
        }
    );
}

has 'timeout' => (
    is => 'rw',
    isa => 'Int',
    required => 1,
    default => 60
);

has 'queue' => (
    is       => 'rw',
    does     => 'Mvalve::Queue',
    required => 1,
    coerce   => 1,
    handles => {
        map { ( "q_$_" => $_ ) }
            qw(next fetch insert clear)
    },
);

__PACKAGE__->meta->make_immutable;

no Moose;

# some special headers
use constant EMERGENCY_HEADER   => 'X-Mvalve-Emergency';
use constant DESTINATION_HEADER => 'X-Mvalve-Destination';
use constant RETRY_HEADER       => 'X-Mvalve-Retry-Time';
use constant DURATION_HEADER    => 'X-Mvalve-Duration';
use constant MVALVE_TRACE       => $ENV{MVALVE_TRACE} ? 1 : 0;

sub trace { print STDERR "MVALVE: @_\n" }

sub next
{
    my $self = shift;

    my $qs    = $self->queue_set;
    my @names = $qs->as_q4m_args;
    my $table = $self->q_next(
        table_conds => \@names, 
        timeout     => $self->timeout + 0
    );

    if (! $table) {
        trace( "q_next did not return a table name, simply returning" ) if MVALVE_TRACE;
        return ();
    }

    trace( "issueing fetch on table '$table'") if MVALVE_TRACE;
    my $message = $self->q_fetch(table => $table);
    if (! $message) {
        trace( "q_fetch did not return a message, simply returning" ) if MVALVE_TRACE;
        return ();
    }

    # destination is an abstract symbol representing the endpoint
    # service name. this /could/ be used by the queue consumer, but it
    # is *not* a
    my $destination = $message->header( DESTINATION_HEADER );

    if ( $qs->is_emergency( $table ) ||  $qs->is_timed( $table ) ) {
        # if this is from an emergency queue or a timed queue, we go ahead
        # and allow the message, but we also update the throttler's count
        # so the next message from a normal queue would be throttled correctly
        if ($message->header(RETRY_HEADER)) {
            $self->state_decr( [ $destination, 'retry' ] );
        }
        $self->try_push( key => $destination );

        return $message;
    }

    # otherwise, we need to check if this message is going to be throttled
    my $is_pending   = $self->is_pending( $destination );
    my $is_throttled = ! $self->try_push( key => $destination );
    trace( "checking if message to $destination should be throttled (pending: $is_pending, throttled: $is_throttled)" ) if MVALVE_TRACE;

    if ($is_throttled || $is_pending) {
        trace( "message", $message->id, "is being throttled") if MVALVE_TRACE;
        $self->defer( $message );
        return (); # no data for you!
    }

    # if we got here, we can just return the data
    trace( "message", $message->id, "being returned") if MVALVE_TRACE;
    return $message;
}

sub defer
{
    my( $self, $message ) = @_;

    if ( ! blessed($message) || ! $message->isa( 'Mvalve::Message' ) ) {
        return () ;
    }

    my $qs          = $self->queue_set;
    my $interval    = $self->throttler->interval;
    my $table       = $qs->choose_table('timed');
    my $destination = $message->header( DESTINATION_HEADER );
    my $time_key    = [ $table, $destination, 'retry time' ];
    my $retry_key   = [ $destination, 'retry' ];

    my $retry = $self->state_get($time_key);
    my $next  = time + $interval;

    if ( ! $retry || $retry < $next ) {
        $retry = $next;
    }
    $message->header( RETRY_HEADER, $retry );

    trace( "defer to $table" ) if MVALVE_TRACE;
    my $rv = $self->q_insert( 
        table => $table,
        data => {
            destination => $destination,
            ready       => int($retry * 1000),
            message     => $message->serialize,
        }
    );

    trace( "q_insert results in $rv" ) if MVALVE_TRACE;

    if ($rv) {
        # duration specifies t
        $retry += $message->header( DURATION_HEADER ) || $interval;
        $self->state_set($time_key, $retry);
        $self->state_incr($retry_key);
    }

    return $rv;
}

sub insert {
    my ($self, %args) = @_;

    my $message = $args{message};

    my $qs = $self->queue_set;

    # Choose one of the queues, depending on the headers
    my $table;
    if ($message->header( EMERGENCY_HEADER ) ) {
        $table = $qs->choose_table( 'emergency' );
    } else {
        $table = $qs->choose_table();
    }

    trace( "insert message '" . $message->id() . "' to $table" ) if MVALVE_TRACE;
    $self->q_insert(
        table => $table,
        data => { 
            destination => $message->header( DESTINATION_HEADER ),
            message => $message->serialize()
        }
    );
}

sub is_pending {
    my( $self, $destination ) = @_;

    my $retry_key = [ $destination, 'retry' ];
    my $count = $self->state_get($retry_key);
    return $count ? 1 : 0;
}

sub clear_all {
    my $self = shift;

    foreach my $table ($self->queue_set->all_tables) {
        $self->q_clear($table);
    }
}

1;

__END__

=head1 NAME

Mvalve - Generic Q4M Powered Message Pipe

=head1 SYNOPSIS

  my $mvalve = Mvalve->new(
    state => {
      module => "...",
    },
    queue => {
      module => "...",
      connect_info => [ ... ]
    },
    throttler => {
      module => 'Data::Valve',
      args => {
        max_items => $max,
        interval  => $interval,
        cache     => {
          data => [ ... ]
        }
      }
    }
  );

  while ( 1 ) {
    my $message = $mvalve->next;
    if ($message) {
      # do whatever
    }
  }

=head1 DESCRIPTION

Mvalve stands for "Messave Valve". It is a frontend for Q4M powered set of
queues, acting as a single pipe.

=head1 SETUP

You need to have installed mysql 5.1 or later and q4m. You can grab
them at:

  http://dev.mysql.com/
  http://q4m.31tools.com/

Once you have a q4m-enabled mysql running, you need to create these q4m 
enabled tables in your mysql database.

  CREATE TABLE q_emerg (
     destination VARCHAR(40) NOT NULL,
     message     BLOB NOT NULL
  ) ENGINE=QUEUE DEFAULT CHARSET=UTF-8
 
  CREATE TABLE q_timed (
     destination VARCHAR(40) NOT NULL,
     ready       BIGINT NOT NULL,
     message     BLOB NOT NULL
  ) ENGINE=QUEUE DEFAULT CHARSET=UTF-8
 
  CREATE TABLE q_incoming (
     destination VARCHAR(40) NOT NULL,
     message     BLOB NOT NULL
  ) ENGINE=QUEUE DEFAULT CHARSET=UTF-8

You also need to setup a memcached compatible distributed cache/storage.
This will be used to share certain key data across multiple instances
of Mvalve.
 
=head1 METHODS

=head2 next

Fetches the next available message. 

=head2 insert 

Inserts into the normal queue

=head2 defer

Inserts in the the retry_wait queue.

=head2 is_pending( $destination )

Checks whethere there are pending retries for that particular destination.

=head2 throttler

C<throttler> holds the Data::Throttler instance that does the dirty work of
determining if a message needs to be throttled or not

  $self->throttler( {
    module => "Data::Throttler::Memcached",
  } );

=head2 timeout

C<timeout> specifies the timeout value while we wait to read from the queue.

=head2 queue

C<queue> is the actual queue instance that we'll be dealing with.
While the architecture is such that you can replace the queue with
your custom object, we currently only support Q4M

  $self->queue( {
    module => "Q4M",
    connect_info => [ 'dbi:mysql:...', ..., ... ]
 } );

=head2 clear_all

Clears all known queues that are listed under the registered QueueSet

=head2 trace

This is for debugging only

=head1 CONSTANTS

=head2 DESTINATION_HEADER

=head2 EMERGENCY_HEADER

=head2 MVALVE_TRACE

=head2 RETRY_HEADER

=head2 DURATION_HEADER

=head1 AUTHORS

Daisuke Maki C<< <daisuke@endeworks.jp> >>

Taro Funaki C<< <t@33rpm.jp> >>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See http://www.perl.com/perl/misc/Artistic.html

=cut

