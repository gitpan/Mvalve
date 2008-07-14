use strict;
use inc::Module::Install;

name('Mvalve');
all_from('lib/Mvalve.pm');

requires('Cache::Memcached');
requires('Data::Serializer');
requires('Data::Valve', '0.00003');
requires('Digest::SHA1');
requires('HTTP::Headers');
requires('Moose');
requires('MooseX::ClassAttribute');
requires('Queue::Q4M', '0.00009');
requires('UNIVERSAL::require');

test_requires('Test::More');
test_requires('Test::Compile');

auto_include;
WriteAll;