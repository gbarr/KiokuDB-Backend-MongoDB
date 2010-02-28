#!/usr/bin/perl

use Test::More;

use ok 'KiokuDB';
use ok 'KiokuDB::Backend::MongoDB';
use ok 'KiokuDB::Backend::Hash';

use KiokuDB::Test;

 my $mdb = KiokuDB::Backend::Hash->new;
#        run_all_fixtures( KiokuDB->new(backend => $mdb));

 $mdb = KiokuDB::Backend::MongoDB->new;
$mdb->clear;
        run_all_fixtures( KiokuDB->new(backend => $mdb));


done_testing;

