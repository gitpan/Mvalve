# $Id$

package Mvalve::State;
use Moose::Role;

requires qw(get set remove incr decr);

no Moose;

1;