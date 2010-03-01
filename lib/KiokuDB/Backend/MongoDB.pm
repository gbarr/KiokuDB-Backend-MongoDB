package KiokuDB::Backend::MongoDB;

use Moose;

use MongoDB;
use JSON;
use boolean;
use Data::Stream::Bulk::Callback;

use namespace::clean -except => 'meta';

with qw(
  KiokuDB::Backend
  KiokuDB::Backend::Serialize::JSPON
  KiokuDB::Backend::Role::UnicodeSafe
  KiokuDB::Backend::Role::Query::Simple
  KiokuDB::Backend::Role::Clear
  KiokuDB::Backend::Role::Scan
  KiokuDB::Backend::Role::TXN::Memory
  KiokuDB::Backend::Role::Concurrency::POSIX
);

has '+id_field'         => (default => '_id');
has '+class_field'      => (default => "class");
has '+class_meta_field' => (default => "class_meta");

has host => (
  isa     => 'Str',
  is      => 'ro',
  default => '127.0.0.1'
);

has database => (
  isa     => 'Str',
  is      => 'ro',
  default => 'kiokudb',
);

has collection => (
  isa     => 'Str',
  is      => 'ro',
  default => 'entries',
);

has port => (
  isa     => 'Int',
  is      => 'ro',
  default => 27017,
);

has storage => (
  isa        => "MongoDB::Collection",
  is         => "ro",
  lazy_build => 1,
);

sub _build_storage {
  my $self = shift;
  MongoDB::Connection->new(host => $self->host, port => $self->port)    ##
    ->get_database($self->database)                                     ##
    ->get_collection($self->collection);
}

sub all_entries {
  shift->simple_search({});
}

sub simple_search {
  my $self   = shift;
  my $query = shift;

  my $cursor = $self->storage->query($query);
  Data::Stream::Bulk::Callback->new(
    callback => sub {
      my $next = $cursor->next or return;
      [$self->txn_loaded_entries($self->expand_jspon($next))];
    }
  );
}

sub clear {
  my $self = shift;
  $self->storage->remove;
}

sub exists {
  my $self = shift;
  map { !!$_ } $self->get_from_storage(@_);
}


sub mdb_collapse_jspon {
    my ($self, $entry) = @_;

    my $jspon = $self->collapse_jspon($entry);

    keys %$jspon; # reset iter
    while(my($k,$v) = each %$jspon) {
      $jspon->{$k} = $v ? boolean::true : boolean::false if JSON::is_bool($v);
    }

    return $jspon;
};

sub get_from_storage {
  my ($self, @ids) = @_;

  my $storage = $self->storage;

  if (@ids) {
    my $csr = $storage->query([_id => {'$in' => \@ids}]);
    my %entries;
    while (my $row = $csr->next) {
      $entries{$row->{_id}} = $self->expand_jspon($row);
    }
    return $self->txn_loaded_entries(@entries{@ids});
  }

  return $self->txn_loaded_entries(map { $self->expand_jspon($_) } $storage->query->all);
}


sub commit_entries {
  my $self = shift;
  return unless @_;

  my $storage = $self->storage;
  my (@delete, @insert);

  foreach my $e (@_) {
    if ($e->deleted) {
      push @delete, $e->id;
    }
    elsif ($e->prev) {
      $storage->update({_id => $e->id}, $self->mdb_collapse_jspon($e), {upsert => 1});
    }
    else {
      push @insert, $self->mdb_collapse_jspon($e);
    }
  }

  if (@insert) {
    $storage->batch_insert(\@insert, {safe => 1})
      or die($storage->_database->last_error->{err} || 'unknown');
  }

  $storage->remove({_id => {'$in' => \@delete}}) if @delete;
}

sub BUILDARGS {
    my $self = shift;
    my $args = $self->SUPER::BUILDARGS(@_);
    $args->{storage}  = delete $args->{collection}    if ref $args->{collection};
    $args->{host}     = delete $args->{database_host} if exists $args->{database_host};
    $args->{port}     = delete $args->{database_port} if exists $args->{database_port};
    $args->{database} = delete $args->{database_name} if exists $args->{database_name};
    $args->{collection} = delete $args->{database_collection}
      if exists $args->{database_collection};
    $args;
}

1;

__END__

=pod

=head1 NAME

KiokuDB::Backend::MongoDB - MongoDB backend for L<KiokuDB>

=head1 SYNOPSIS

    KiokuDB->connect( "MongoDB:hist=localhost;port=12345;database=test;collection=foobar" );

=head1 DESCRIPTION

This backend provides L<KiokuDB> support for MongoDB using L<MongoDB>.

=head1 TRANSACTION SUPPORT

MongoDB does not have any support for transactions or atomic changes
to multiple documents. However transactions are be implemented by
deferring all operations until the final commit.

This means transactions are memory bound so if you are inserting
or modifying lots of data it might be wise to break it down to
smaller transactions.

=head1 ATTRIBUTES

=over 4

=item host

Hostname of machine to connect to. Defaults to C<localhost>

=item port

TCP port to connect to. Defaults to 27017

=item database

Name of database to use. Defaults to C<kiokudb>

=item collection

Name of collection to use. Defaults to C<entries>

=item storage

An L<MongoDB::Collection> instance.

=back

=head1 NOTICE

MongoDB uses perls internal flags on values to determine if the value should be stored
as an integer, real or a string. This can result in what is thought to be a number being
stored as a string. This has an impact when later doing searches on that field.

=head1 SEE ALSO

L<KiokuDB>, L<MongoDB>

=head1 DEVELOPMENT

L<http://github.com/gbarr/KiokuDB-Backend-MongoDB>

=head1 AUTHOR

Graham Barr E<lt>gbarr@pobox.comE<gt>

=head1 LICENSE

This software is copyright (c) 2010 by Graham Barr.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
