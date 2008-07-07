# $Id$

package Mvalve::State::Memory;
use Moose;

with 'Mvalve::State';

has 'data' => (
    is => 'rw',
    isa => 'HashRef',
    default => sub { +{} }
);

no Moose;

sub get { shift->data->{$_[0]} }
sub set { shift->data->{$_[0]} = $_[1] }
sub remove { delete shift->data->{$_[0]} }

sub incr { shift->data->{$_[0]}++ }
sub decr { shift->data->{$_[0]}-- }

1;