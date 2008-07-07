use strict;
use Test::More tests => 8;

BEGIN
{
    use_ok("Mvalve::QueueSet");
}

can_ok( "Mvalve::QueueSet" => qw(
    all_queues as_q4m_args
) );

{
    my $queues = Mvalve::QueueSet->new;

    ok( $queues );
    isa_ok( $queues, "Mvalve::QueueSet" );

    my( @all_queues ) = $queues->all_queues;
    is( @all_queues , 3 );

    my( @args ) = $queues->as_q4m_args;

    is( shift @args, 'q_emerg' );
    is( shift @args, 'q_retry' );
    is( shift @args, 'q_incoming' );
}
