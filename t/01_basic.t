use strict;
use Test::More;

BEGIN
{
    if (! $ENV{MVALVE_Q4M_DSN} ) {
        plan(skip_all => "Define MVALVE_Q4M_DSN to run this test");
    } else {
        plan(tests => 4);
    }

    use_ok( "Mvalve" );
}

{
    my $mv = Mvalve->new(
        throttler => {
            args => {
                max_items => 10,
                interval  => 20
            }
        },
        queue => {
            args => {
                connect_info => [ 
                    $ENV{MVALVE_Q4M_DSN},
                    $ENV{MVALVE_Q4M_USERNAME},
                    $ENV{MVALVE_Q4M_PASSWORD},
                    { RaiseError => 1, AutoCommit => 1 },
                ]
            }
        }
    );
    ok( $mv );
    isa_ok( $mv, 'Mvalve' );

    my $message = Mvalve::Message->new(
        headers => {
            'X-Mvalve-Destination' => 'test'
        },
        content => "test"
    );

    $mv->insert( message => $message );

    {
        my $rv = $mv->next();
        ok($rv);
    }
}