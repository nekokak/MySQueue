use strict;
use warnings;
use Test::More;
use Test::mysqld;
use DBI;
use MySQueue;
use Data::Dumper;
use Data::MessagePack;

my $mysql = Test::mysqld->new({
    my_cnf => {
        'skip-networking' => '',
    }
}) or plan skip_all => $Test::mysqld::errstr;

my $dbh = DBI->connect($mysql->dsn( dbname => "test" ), '','',{ RaiseError => 1, PrintError => 0, AutoCommit => 1 });

my $mq = MySQueue->new($dbh);

# initialize data
do {
    $dbh->do(q{
        CREATE TABLE job (
            id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
            arg           MEDIUMBLOB,
            status        VARCHAR(10) DEFAULT 'wait',
            grabbed_until INTEGER UNSIGNED NOT NULL
        ) ENGINE=InnoDB
    });
    $dbh->do(q{SET GLOBAL event_scheduler = ON});
    $dbh->do(q{
        CREATE EVENT job_event ON SCHEDULE EVERY 3 SECOND
        DO
            UPDATE job SET status = 'wait' WHERE status = 'started' AND grabbed_until <= UNIX_TIMESTAMP()
        ;
    });
};

sub _check_db_data { $dbh->selectall_arrayref('SELECT * FROM job', +{ Slice => +{}}) }

subtest 'enqueue' => sub {

    $mq->enqueue('job', +{name => 'nekokak'});
    my $rows = _check_db_data();
    is scalar(@$rows), 1;
    is $rows->[0]->{id}, 1;
    is_deeply +Data::MessagePack->unpack($rows->[0]->{arg}), +{name => 'nekokak'};

    $mq->enqueue('job', +{name => 'zigorou'});
    my $db_rows = _check_db_data();
    is scalar(@$db_rows), 2;
    is $db_rows->[1]->{id}, 2;
    is_deeply +Data::MessagePack->unpack($db_rows->[1]->{arg}), +{name => 'zigorou'};
};

subtest 'dequeue / get job' => sub {
    my $rows = $mq->dequeue('job', +{job_limit => 1, grab_for => 1});

    note explain $rows;

    is scalar(@$rows), 1;
    is $rows->[0]->{id}, 1;
    is_deeply $rows->[0]->{arg}, +{name => 'nekokak'};

    my $db_rows = _check_db_data();
    is scalar(@$db_rows), 2;
    is $db_rows->[0]->{id}, 1;
    is_deeply +Data::MessagePack->unpack($db_rows->[0]->{arg}), +{name => 'nekokak'};
    is $db_rows->[0]->{status}, 'started';

    is $db_rows->[1]->{id}, 2;
    is_deeply +Data::MessagePack->unpack($db_rows->[1]->{arg}), +{name => 'zigorou'};
    is $db_rows->[1]->{status}, 'wait';
};

subtest 'dequeue / get job' => sub {
    my $rows = $mq->dequeue('job', +{job_limit => 1, grab_for => 1});

    note explain $rows;

    is scalar(@$rows), 1;
    is $rows->[0]->{id}, 2;
    is_deeply $rows->[0]->{arg}, +{name => 'zigorou'};

    my $db_rows = _check_db_data();
    is scalar(@$db_rows), 2;
    is $db_rows->[0]->{id}, 1;
    is_deeply +Data::MessagePack->unpack($db_rows->[0]->{arg}), +{name => 'nekokak'};
    is $db_rows->[0]->{status}, 'started';

    is $db_rows->[1]->{id}, 2;
    is_deeply +Data::MessagePack->unpack($db_rows->[1]->{arg}), +{name => 'zigorou'};
    is $db_rows->[1]->{status}, 'started';
};

subtest 'dequeue / do not get job' => sub {
    ok not $mq->dequeue('job', +{job_limit => 1, grab_for => 1});
};

sleep 3;

subtest 'dequeue / get job' => sub {
    my $rows = $mq->dequeue('job', +{job_limit => 1, grab_for => 1});

    note explain $rows;

    is scalar(@$rows), 1;
    is $rows->[0]->{id}, 1;
    is_deeply $rows->[0]->{arg}, +{name => 'nekokak'};

    my $db_rows = _check_db_data();
    is scalar(@$db_rows), 2;
    is $db_rows->[0]->{id}, 1;
    is_deeply +Data::MessagePack->unpack($db_rows->[0]->{arg}), +{name => 'nekokak'};
    is $db_rows->[0]->{status}, 'started';

    is $db_rows->[1]->{id}, 2;
    is_deeply +Data::MessagePack->unpack($db_rows->[1]->{arg}), +{name => 'zigorou'};
    is $db_rows->[1]->{status}, 'wait';
};

done_testing;

