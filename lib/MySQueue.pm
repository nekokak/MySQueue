package MySQueue;
use strict;
use warnings;
our $VERSION = '0.01';
use Data::MessagePack;

sub new {
    my ($class, $dbh) = @_;
    bless {
        dbh => $dbh,
    }, $class;
}

sub dbh { $_[0]->{dbh} }

sub enqueue {
    my ($self, $table, $arg) = @_;

    $self->dbh->do(sprintf(q{INSERT INTO %s (arg, grabbed_until) VALUES (?,UNIX_TIMESTAMP())}, $table), undef, Data::MessagePack->pack($arg));
}

sub dequeue {
    my ($self, $table, $opts) = @_;

    my $job_limit = $opts->{job_limit} || 100;

    my $rows = $self->dbh->selectall_arrayref(
        sprintf(q{SELECT id, arg FROM %s WHERE status = 'wait' ORDER BY id ASC LIMIT ? OFFSET 0}, $table),
        +{ Slice => +{}},
        $job_limit
    );

    return unless scalar(@$rows);

    my $grab_for = $opts->{grab_for} || 60;
    my @ids = map { $_->{id} } @$rows;

    $self->dbh->do(
        sprintf(
            q{UPDATE %s SET status = 'started', grabbed_until = (UNIX_TIMESTAMP() + ? ) WHERE id IN (%s)},
            $table,
            substr('?,' x scalar(@ids), 0, -1),
        ),
        undef, ($grab_for, @ids)
    );

    map {$_->{arg} = Data::MessagePack->unpack($_->{arg})} @$rows;

    return $rows;
}

1;
__END__

=head1 NAME

MySQueue -

=head1 SYNOPSIS

  use MySQueue;

=head1 DESCRIPTION

MySQueue is

=HEAD1 SCHEMA

  CREATE TABLE job (
      id            BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
      arg           MEDIUMBLOB,
      status        VARCHAR(10) DEFAULT 'wait',
      grabbed_until INTEGER UNSIGNED NOT NULL
  ) ENGINE=InnoDB

  SET GLOBAL event_scheduler = ON;
  DROP EVENT IF EXISTS job_event;
  CREATE EVENT job_event ON SCHEDULE EVERY 10 SECOND
  DO 
   UPDATE mock SET status = 'wait' WHERE status = 'started' AND grabbed_until <= UNIX_TIMESTAMP()
  ;

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
