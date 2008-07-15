# $Id: /mirror/coderepos/lang/perl/Mvalve/trunk/lib/Mvalve/Writer.pm 65693 2008-07-15T01:07:26.094046Z daisuke  $

package Mvalve::Writer;
use Moose;
use Mvalve::Const;
use Mvalve::Types;
use Mvalve::Message;

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

has 'queue_set' => (
    is  => 'ro',
    isa => 'Mvalve::QueueSet',
    default => sub {
        my $class = 'Mvalve::QueueSet';
        Class::MOP::load_class($class);
        $class->new;
    }
);


__PACKAGE__->meta->make_immutable;

no Moose;

sub clear_all {
    my $self = shift;

    foreach my $table ($self->queue_set->all_tables) {
        $self->q_clear($table);
    }
}

sub insert {
    my ($self, %args) = @_;

    my $message = $args{message};

    my $qs = $self->queue_set;

    # Choose one of the queues, depending on the headers
    my $table;
    if ($message->header( &Mvalve::Const::EMERGENCY_HEADER ) ) {
        $table = $qs->choose_table( 'emergency' );
    } else {
        $table = $qs->choose_table();
    }

    Mvalve::trace( "insert message '" . $message->id() . "' to $table" )
        if &Mvalve::Const::MVALVE_TRACE;

    $self->q_insert(
        table => $table,
        data => { 
            destination => $message->header( &Mvalve::Const::DESTINATION_HEADER ),
            message => $message->serialize()
        }
    );
}

1;

__END__

=head1 NAME

Mvalve::Writer - Mvalve Writer

=head1 METHODS

=head2 insert 

Inserts into the normal queue

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