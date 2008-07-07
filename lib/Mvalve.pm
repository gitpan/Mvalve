# $Id$

package Mvalve;
use Moose;
use Moose::Util::TypeConstraints;
use Mvalve::Message;
use Time::HiRes();

our $VERSION   = '0.00005';
our $AUTHORITY = "cpan:DMAKI";

class_type 'Data::Throttler';
role_type 'Mvalve::Queue';
role_type 'Mvalve::State';

coerce 'Data::Throttler'
    => from 'HashRef'
        => via {
            my $h = $_;
            my $module = delete $h->{module} || 'Data::Throttler';
            Class::MOP::load_class($module);

            $module->new(%$h);
        }
;

coerce 'Mvalve::Queue'
    => from 'HashRef'
        => via {
            my $h = $_;
            my $module = delete $h->{module} || 'Mvalve::Queue::Q4M';
            Class::MOP::load_class($module);

            $module->new(%$h);
        }
;

coerce 'Mvalve::State'
    => from 'HashRef'
        => via {
            my $h = $_;
            my $module = delete $h->{module} || 'Mvalve::State::Memory';
            Class::MOP::load_class($module);

            $module->new(memcached => $h);
        }
;

has 'throttler' => (
    is       => 'rw',
    isa      => 'Data::Throttler',
    required => 1,
    coerce   => 1,
    handles  => [ qw(try_push) ],
);

has 'queue_set' => (
    is  => 'ro',
    isa => 'Mvalve::QueueSet',
    default => sub {
        Class::MOP::load_class('Mvalve::QueueSet');
        Mvalve::QueueSet->new;
    }
);

has 'state' => (
    is => 'rw',
    does => 'Mvalve::State',
    coerce => 1,
    required => 1,
    default => sub { 
        Class::MOP::load_class('Mvalve::State::Memory');
        Mvalve::State::Memory->new
    },
    handles => {
        map { ("state_$_" => $_) } qw(get set remove incr decr)
    }
);

has 'interval' => (
    is => 'rw',
    isa => 'Int',
    required => 1,
    default => 10
);

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

use constant EMERGENCY_HEADER   => 'X-Mvalve-Emergency';
use constant DESTINATION_HEADER => 'X-Mvalve-Destination';
use constant RETRY_HEADER       => 'X-Mvalve-Retry-Time';
use constant MVALVE_TRACE       => $ENV{MVALVE_TRACE} ? 1 : 0;

sub trace
{
    print STDERR "MVALVE: @_\n";
}

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

    if ( $qs->is_emergency( $table ) ||  $qs->is_retry( $table ) ) {
        # if this is from an emergency queue or a retry queue, we go ahead
        # and allow the message, but we also update the throttler's count
        # so the next message from a normal queue would be throttled correctly
        $self->try_push( key => $destination );

        return $message;
    }


    # otherwise, we need to check if this message is going to be throttled
    trace( "checking if message to $destination should be throttled" ) if MVALVE_TRACE;
    my $is_throttled =
        $self->is_pending( $destination ) ||
        ! $self->try_push( key => $destination )
    ;

    if ($is_throttled) {
        trace( "message", $message->id, "is being throttled") if MVALVE_TRACE;
        $self->defer( $message );
        return (); # no data for you!
    }

    # if we got here, we can just return the data
    trace( "message", $message->id, "being returned") if MVALVE_TRACE;
    return $message;
}

sub next_retry
{
    my $self = shift;

    my $time = Time::HiRes::time();
    my $retry_table = $self->queue_set->choose_table('retry_wait');
    my $cond = sprintf( "%s:retry<=%d", $retry_table, $time );

    trace( "next_retry() for table $retry_table, where retry time is <= $time ($cond)" ) if MVALVE_TRACE;

    my $table = $self->q_next(table_conds => [ $cond ], timeout => $self->timeout);
    if (! $table) {
        trace( "next_retry() did not return anything" ) if MVALVE_TRACE;
        Time::HiRes::usleep(100);
        return ();
    }

    my $message = $self->q_fetch( table => $table );
    $self->insert_retry( $message );
}

sub defer
{
    my( $self, $message ) = @_;

    if ( ! blessed($message) || ! $message->isa( 'Mvalve::Message' ) ) {
        return () ;
    }

    my $qs          = $self->queue_set;
    my $interval    = $self->interval;
    my $table       = $qs->choose_table('retry_wait');
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
            retry       => $retry,
            message     => $message->serialize,
        }
    );

    trace( "q_insert results in $rv" ) if MVALVE_TRACE;

    if ($rv) {
        # duration specifies t
        $retry += $message->header('x-wfg-duration') ||
                  $interval;
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

sub insert_retry
{
    my( $self, $message ) = @_;

    if ( ! blessed($message) || ! $message->isa( 'Mvalve::Message' ) ) {
        return () ;
    }

    my $table = $self->queue_set->choose_table('retry');
    my $channel_id = $message->header( DESTINATION_HEADER );

    my $rv = $self->q_insert(
        table => $table, 
        data => {
            destination => $message->header( DESTINATION_HEADER ),
            message => $message->serialize(),
        }
    );

    if ($rv) {
        my $retry_key = [ $channel_id, 'retry' ];
        my $count = $self->state_decr($retry_key) || 0;
        if ( $count < 0 ) {
            $self->state_set($retry_key, 0);
        }
    }

    return $rv;
}

sub is_pending {
    my( $self, $channel_id ) = @_;

    my $retry_key = [ $channel_id, 'retry' ];
    my $count = $self->state_get($retry_key);
    return $count ? 1 : 0;
}

sub clear_all {
    my $self = shift;

    foreach my $table ($self->queue_set->all_tables) {
        $self->q_clear($table);
    }
}

__END__

=head1 NAME

Mvalve - Generic Q4M Powered Message Pipe

=head1 SYNOPSIS

  my $mvalve = Mvalve->new(
    throttler => {
      module => 'Data::Throttler::Memcached',
      max_items => $max,
      interval  => $interval,
      cache     => {
        data => [ ... ]
      }
    }
  );

  while ( 1 ) {
    my $message = $throttler->next;
    if ($message) {
      # do whatever
    }
  }

=head1 METHODS

=head2 next

Fetches the next available message. 

=head2 insert 

Inserts into the normal queue

=head2 next_retry

Fetches the next message waiting to be requeued in retry_wait queue 
to the retry queue.

=head2 insert_retry

Inserts into the retry queue, noting the next fetch time.

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

=head1 AUTHORS

Daisuke Maki C<< <daisuke@endeworks.jp> >>

Taro Funaki C<< <t@33rpm.jp> >>

=cut

