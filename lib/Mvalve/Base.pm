# $Id: /mirror/coderepos/lang/perl/Mvalve/trunk/lib/Mvalve/Base.pm 65775 2008-07-15T06:15:16.660072Z daisuke  $

package Mvalve::Base;
use Moose;

with 'MooseX::KeyedMutex';

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

sub defer
{
    my( $self, %args ) = @_;

    my $message = $args{message};
    my $interval = $args{interval} || 
        $message->header( &Mvalve::Const::DURATION_HEADER ) ||
        0;


    if ( ! blessed($message) || ! $message->isa( 'Mvalve::Message' ) ) {
        return () ;
    }

    my $qs          = $self->queue_set;
    my $table       = $qs->choose_table('timed');
    my $destination = $message->header( &Mvalve::Const::DESTINATION_HEADER );
    my $time_key    = [ $table, $destination, 'retry time' ];
    my $retry_key   = [ $destination, 'retry' ];

    my $done = 0;
    my $rv;
    while (! $done) {
        my $lock = $self->lock( join('.', @$time_key ) );
        next unless $lock;

        $done = 1;

        my $retry = $self->state_get($time_key);
        my $next  = (time + $interval) * 1000;

        if ( ! $retry || $retry < $next ) {
            $retry = $next;
        }
        $message->header( &Mvalve::Const::RETRY_HEADER, $retry );

        Mvalve::trace( "defer to $table (retry = $retry)" ) if &Mvalve::Const::MVALVE_TRACE;
        $rv = $self->q_insert( 
            table => $table,
            data => {
                destination => $destination,
                ready       => $retry,
                message     => $message->serialize,
            }
        );

        Mvalve::trace( "q_insert results in $rv" ) if &Mvalve::Const::MVALVE_TRACE;

        if ($rv) {
            $retry += $interval;
            $self->state_set($time_key, $retry);
        }
    }

    return $rv;
}

1;

__END__

=head1 NAME

Mvalve::Base - Base Class For Mvalve Reader/Writer

=head1 METHODS

=head2 defer

Inserts in the the retry_wait queue.

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