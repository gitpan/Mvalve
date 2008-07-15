# $Id$

package Mvalve::Reader;
use Moose;
use Mvalve;
use Mvalve::Const;
use Mvalve::Types;

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

__PACKAGE__->meta->make_immutable;

no Moose;

sub clear_all {
    my $self = shift;

    foreach my $table ($self->queue_set->all_tables) {
        $self->q_clear($table);
    }
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
        Mvalve::trace( "q_next did not return a table name, simply returning" ) if &Mvalve::Const::MVALVE_TRACE;
        return ();
    }

    Mvalve::trace( "issueing fetch on table '$table'") if &Mvalve::Const::MVALVE_TRACE;
    my $message = $self->q_fetch(table => $table);
    if (! $message) {
        Mvalve::trace( "q_fetch did not return a message, simply returning" ) if &Mvalve::Const::MVALVE_TRACE;
        return ();
    }

    # destination is an abstract symbol representing the endpoint
    # service name. this /could/ be used by the queue consumer, but it
    # is *not* a
    my $destination = $message->header( &Mvalve::Const::DESTINATION_HEADER );

    if ( $qs->is_emergency( $table ) ||  $qs->is_timed( $table ) ) {
        # if this is from an emergency queue or a timed queue, we go ahead
        # and allow the message, but we also update the throttler's count
        # so the next message from a normal queue would be throttled correctly
        if ($message->header(&Mvalve::Const::RETRY_HEADER)) {
            $self->state_decr( [ $destination, 'retry' ] );
        }
        $self->try_push( key => $destination );

        return $message;
    }

    # otherwise, we need to check if this message is going to be throttled
    my $is_pending   = $self->is_pending( $destination );
    my $is_throttled = ! $self->try_push( key => $destination );
    Mvalve::trace( "checking if message to $destination should be throttled (pending: $is_pending, throttled: $is_throttled)" ) if &Mvalve::Const::MVALVE_TRACE;

    if ($is_throttled || $is_pending) {
        Mvalve::trace( "message", $message->id, "is being throttled") if &Mvalve::Const::MVALVE_TRACE;
        $self->defer( $message );
        return (); # no data for you!
    }

    # if we got here, we can just return the data
    Mvalve::trace( "message", $message->id, "being returned") if &Mvalve::Const::MVALVE_TRACE;
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
    my $destination = $message->header( &Mvalve::Const::DESTINATION_HEADER );
    my $time_key    = [ $table, $destination, 'retry time' ];
    my $retry_key   = [ $destination, 'retry' ];

    my $retry = $self->state_get($time_key);
    my $next  = time + $interval;

    if ( ! $retry || $retry < $next ) {
        $retry = $next;
    }
    $message->header( &Mvalve::Const::RETRY_HEADER, $retry );

    Mvalve::trace( "defer to $table" ) if &Mvalve::Const::MVALVE_TRACE;
    my $rv = $self->q_insert( 
        table => $table,
        data => {
            destination => $destination,
            ready       => int($retry * 1000),
            message     => $message->serialize,
        }
    );

    Mvalve::trace( "q_insert results in $rv" ) if &Mvalve::Const::MVALVE_TRACE;

    if ($rv) {
        # duration specifies t
        $retry += $message->header( &Mvalve::Const::DURATION_HEADER ) || $interval;
        $self->state_set($time_key, $retry);
        $self->state_incr($retry_key);
    }

    return $rv;
}

sub is_pending {
    my( $self, $destination ) = @_;

    my $retry_key = [ $destination, 'retry' ];
    my $count = $self->state_get($retry_key);
    return $count ? 1 : 0;
}

1;

__END__

=head1 NAME

Mvalve::Reader - Mvalve Reader

=head1 METHODS

=head2 next

Fetches the next available message. 

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

=head2 clear_all

Clears all known queues that are listed under the registered QueueSet

=head2 queue

C<queue> is the actual queue instance that we'll be dealing with.
While the architecture is such that you can replace the queue with
your custom object, we currently only support Q4M

  $self->queue( {
    module => "Q4M",
    connect_info => [ 'dbi:mysql:...', ..., ... ]
 } );

=cut

