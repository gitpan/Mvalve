# $Id$

package Mvalve::Queue::Q4M;
use Moose;
use Queue::Q4M;

with 'Mvalve::Queue';

has 'connect_info' => (
    is => 'rw',
    isa => 'ArrayRef',
    required => 1
);

has 'q4m' => (
    is       => 'rw',
    isa      => 'Queue::Q4M',
    handles  => {
        map { ("q_$_" => $_) } 
            qw( fetch_hashref )
    }
);

__PACKAGE__->meta->make_immutable;

no Moose;

sub BUILD {
    my $self = shift;
    $self->q4m( 
        Queue::Q4M->connect( connect_info => $self->connect_info )
    );
    $self;
}

sub next {
    my ($self, %args) = @_;
    my $table_conds = $args{table_conds};
    if (! $table_conds) {
        return ();
    }

    my @args = @$table_conds;
    if ($args{timeout} > 0) {
        push @args, $args{timeout};
    }
    return $self->q4m->next(@args);
}

sub fetch {
    my ($self, %args) = @_;

    my $table = $args{table};
    my $args  = $args{args};

    my $cols = $self->q_fetch_hashref( $table, $args );
    if (! $cols) {
        return ();
    }

    return Mvalve::Message->deserialize( $cols->{message} );
}

sub insert {
    my ($self, %args) = @_;
    $self->q4m->insert( $args{table}, $args{data} );
}

sub clear { shift->q4m->clear(@_) }


1;
