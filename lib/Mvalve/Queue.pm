# $Id$

package Mvalve::Queue;
use Moose::Role;

requires qw(next fetch insert);

no Moose;

1;