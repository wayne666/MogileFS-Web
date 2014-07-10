#!/usr/bin/perl
use strict;
use warnings;
use Mojolicious::Lite;
use DBIx::Custom;
use Smart::Comments '###';
use Mojo::IOLoop;

# DBIx::Custom
$ENV{DBIX_CUSTOM_DEBUG} = 1;
my $dbi = DBIx::Custom->connect(
    dsn => 'dbi:mysql:MogileFS:192.168.1.141:3306', 
    user => 'readonly',
    password => 'read%141',  
    option => {mysql_enable_utf8 => 1},  
    connector => 1,
);

$dbi->async_conf({
    prepare_attr => {async => 1},  
    fh => sub { shift->dbh->mysql_fd }
});

get '/test' => sub {
    my $self = shift;
    ### test
    $self->render('test');
};

### index and search
get '/' => sub {
    my $self = shift;
    my $offset = 50;
    my $search_key = $self->param('searchKey');

    my $sql;
    if ($search_key) {
        if ($search_key =~ /^\d+$/) {
            $sql = "select a.fid, a.dkey, a.length, a.devcount, b.namespace from file as a, domain as b where a.dmid=b.dmid and a.fid='$search_key'"; 
        }
        else {
            $sql = "select a.fid, a.dkey, a.length, a.devcount, b.namespace from file as a, domain as b where a.dmid=b.dmid and a.dkey='$search_key'"; 
        }
    }
    else {
        $sql = "select a.fid, a.dkey, a.length, a.devcount, b.namespace from file as a, domain as b where a.dmid=b.dmid limit $offset"; 
    }

    $self->render_later;
    $dbi->execute(
           $sql,
           undef,
           prepare_attr => {async => 1}, statement => 'select',
           async => sub {
               my ($dbi, $result) = @_;
               my $rows = $result->all;
               my %indexPage;
               my $fid;
               foreach my $row (@$rows) {
                   my $devcount  = $row->{devcount};
                   my $dkey      = $row->{dkey};
                      $fid       = $row->{fid};
                   my $length    = $row->{'length'};
                   my $namespace = $row->{namespace};

                   $indexPage{$fid} = $row;
               }

               $self->render(
                   'index', 
                   indexPage => \%indexPage,
               );
           }
       ); 

};

get '/detail/:fid' => sub {
    my $self = shift;
    my $fid  = $self->param('fid');
    
    Mojo::IOLoop->delay(

        sub {
            my $delay = shift;
            $dbi->execute(
                   "select a.fid, a.dkey, a.length, a.devcount, b.namespace from file as a, domain as b where a.dmid=b.dmid and fid=$fid",
                   undef,
                   prepare_attr => {async => 1}, statement => 'select',
                   async => $delay->begin,
            );

            $dbi->execute(
                "select a.fid, a.devid, c.hostname, c.hostip from file_on as a, device as b, host as c where a.devid=b.devid and b.hostid=c.hostid and a.fid=$fid",     
                undef,
                prepare_attr => {async => 1}, statement => 'select',
                async => $delay->begin,
            );      
        }, 

        sub {
            my ($delay, $index, $detail) = @_;
            my $ref_index  = $index->one;
            my $ref_detail = $detail->all;
            
            my $count = 1;
            foreach my $row (@$ref_detail) {
                $ref_index->{"hostip$count"}   = $row->{hostip};
                $ref_index->{"devid$count"}      = $row->{devid};
                $ref_index->{"hostname$count"} = $row->{hostname};
                $count++;
            }
            
            $self->render( json => {fileDetail => $ref_index} );

        },
    );

    $self->render_later;


};

get '/hostlist' => sub {
    my $self = shift;

    $self->render_later;
    $dbi->execute(
        "select * from host",
        undef,
        prepare_attr => {async => 1}, statement => 'select',
        async => sub {
            my ($dbi, $result) = @_;
            my $ref_hosts = $result->all;

            my @hosts;
            foreach my $row (@$ref_hosts) {
                my %host;
                $host{hostip}    = $row->{hostip};
                $host{hostname}  = $row->{hostname};
                $host{http_port} = $row->{http_port};
                $host{status}    = $row->{status};
                push @hosts, \%host;
            }
            $self->render('hostlist', hosts => \@hosts);
        }
    );
};

get '/devlist' => sub {
    my $self = shift;
    my $offset = 30;
    my $count = $dbi->count(table => 'device');
    my $pageCount = $count % $offset == 0 ? $count / $offset : int($count / $offset) + 1;

    $self->render_later;
    $dbi->execute(
        "select a.devid, a.status, b.hostname, b.hostip from device as a, host as b where a.hostid=b.hostid order by a.devid limit $offset",
        undef,
        prepare_attr => {async => 1}, statement => 'select',
        async => sub {
            my ($dbi, $result) = @_;
            my $ref_dev = $result->all;

            $self->render(devices => $ref_dev, pageTotal => $pageCount );
        }
    );
};

get '/devpage/:pagenum' => sub {
    my $self = shift;
    my $offset = 30;
    my $pagenum = $self->param('pagenum') || 1;
    my $start = ($pagenum - 1) * $offset;
    my $count = $dbi->count(table => 'device');
    my $pageCount = $count % $offset == 0 ? $count / $offset : int($count / $offset) + 1;

    $self->render_later;
    $dbi->execute(
        "select a.devid, a.status, b.hostname, b.hostip from device as a, host as b where a.hostid=b.hostid order by a.devid limit $start, $offset",
        undef,
        prepare_attr => {async => 1}, statement => 'select',
        async => sub {
            my ($dbi, $result) = @_;
            my $ref_dev = $result->all;

            $self->render(json => { devices => $ref_dev, pageTotal => $pageCount });
        }
    );

};

get '/domainlist' => sub {
    my $self = shift;

    $self->render_later;
    $dbi->execute(
        "select * from domain order by dmid",
        undef,
        prepare_attr => {async => 1}, statement => 'select',
        async => sub {
            my ($dbi, $result) = @_;
            my $ref_domain = $result->all;

            my @domains;
            foreach my $row (@$ref_domain) {
                my $domain = $row->{namespace};
                next if $domain eq 'test';
                push @domains, {domain => $domain, id => $row->{dmid}};
            }
            $self->render('domainlist', domains => \@domains);
        }
    );
    
};

get '/page/:pagenum' => sub {
    my $self = shift;
    my $pagenum = $self->param('pagenum') || 1;

    my $offset = 50;
    my $start_page = ($pagenum - 1) * $offset;

    $self->render_later;
    $dbi->execute(
           "select a.fid, a.dkey, a.length, a.devcount, b.namespace from file as a, domain as b where a.dmid=b.dmid limit $start_page, $offset",
           undef,
           prepare_attr => {async => 1}, statement => 'select',
           async => sub {
               my ($dbi, $result) = @_;
               my $rows = $result->all;
               my %indexPage;
               my @indexPage;
               my $fid;
               foreach my $row (@$rows) {
                   my $devcount  = $row->{devcount};
                   my $dkey      = $row->{dkey};
                      $fid       = $row->{fid};
                   my $length    = $row->{'length'};
                   my $namespace = $row->{namespace};

                   #$indexPage{$fid} = $row;
                   push @indexPage, $row;
               }

               ### %indexPage
               $self->render(
                   json => { indexPage => \@indexPage},
               );
           }
    );
};


get '/search' => sub {
    my $self = shift;
};

app->start;
