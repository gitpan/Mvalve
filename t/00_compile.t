
use Test::More;
BEGIN
{
    eval { require Test::Compile; Test::Compile->import };
    if ($@) {
        skip_all( "Test::Compile required for testing compilation: $@");
    }
}
Test::Compile::all_pm_files_ok();