use inc::Module::Install;
name 'MySQueue';
all_from 'lib/MySQueue.pm';

#requires '';

test_requires 'Test::More' => '0.94';

tests 't/*.t t/*/*.t';
author_tests('xt');

auto_set_repository;

WriteAll;
