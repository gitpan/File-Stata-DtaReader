#!/usr/bin/perl -w

package File::Stata::DtaReader;

=head1 NAME

File::Stata::DtaReader - read Stata 8 and Stata 10 .dta files

=head1 SYNOPSIS

=over 8

=item open FILE, '<', 'test.dta';

=item my $dta = new File::Stata::DtaReader(*FILE);

=item print STDERR "$dta->{nvar} vars; $dta->{nobs} obs\n";

=item print join( ',', @{ $dta->{varlist} } ) . "\n";

=item while ( my @a = $dta->readRow ) { print join( ',', @a ) . "\n"; }

=back

=head1 BUGS

This is quick and dirty early version (November 2007).  Not much testing done.

The bug in version 0.092 relating to handling of 0 float/double values was fixed in version 0.2.

All types of Stata missing values (determined somewhat
approximately in the case of float and double) are rendered
as a perl undef.

=head1 AUTHOR

Written by Franck Latremoliere. Copyright (c) 2007 Reckon LLP.
See http://www.reckon.co.uk/staff/franck/ for more information.

=head1 LICENCE

You may use or redistribute this module under the same terms as perl itself
(Artistic Licence or GNU GPL).

=cut

BEGIN {

    $File::Stata::DtaReader::VERSION = '0.2';

    # test for float endianness using little-endian 33 33 3b f3, which is a float code for 1.4
    my $testFloat = unpack( 'f', pack( 'h*', 'f33b3333' ) );
    $File::Stata::DtaReader::byteOrder = 1 if ( 2.0 * $testFloat > 2.7 && 2.0 * $testFloat < 2.9 );
    $testFloat = unpack( 'f', pack( 'h*', '33333bf3' ) );
    $File::Stata::DtaReader::byteOrder = 2 if ( 2.0 * $testFloat > 2.7 && 2.0 * $testFloat < 2.9 );
    warn "Unable to detect endianness of float storage" unless $File::Stata::DtaReader::byteOrder;
}

sub new($$) {
    my $className  = shift;
    my $fileHandle = shift;
    my $self       = { fh => $fileHandle };
    bless $self, $className;
    $self->readHeader;
    if ( $self->{ds_format} == 114 || $self->{ds_format} == 113 ) {
        $self->readDescriptors;
        $self->readVariableLabels;
        $self->discardExpansionFields;
        $self->prepareDataReader;
    }
    return $self;
}

sub readHeader($) {
    my $self = shift;
    local $_;
    read $self->{fh}, $_, 4;
    ( $self->{ds_format}, $self->{byteorder}, $self->{filetype}, $_ ) = unpack( 'CCCC', $_ );
    read $self->{fh}, $_, 105;
    ( $self->{nvar}, $self->{nobs}, $self->{data_label}, $self->{time_stamp} ) =
      unpack( ( $self->{byteorder} == 2 ? 'vV' : 'nN' ) . 'A81A18', $_ );
    $self->{data_label} =~ s/\x00.*$//s;
    $self->{time_stamp} =~ s/\x00.*$//s;
}

sub readDescriptors($) {
    my $self = shift;
    my $nv   = $self->{nvar};
    local $_;
    read $self->{fh}, $_, $nv;
    $self->{typlist} = [ unpack( 'C' x $nv, $_ ) ];
    read $self->{fh}, $_, $nv * 33;
    $self->{varlist} = [ map { s/\x00.*$//s; $_ } unpack( 'A33' x $nv, $_ ) ];
    read $self->{fh}, $_, $nv * 2 + 2;
    $self->{srtlist} = [ unpack( ( $self->{byteorder} == 2 ? 'v' : 'n' ) x ( 1 + $nv ), $_ ) ];
    my $fmtSize = $self->{ds_format} == 113 ? 12 : 49;
    read $self->{fh}, $_, $nv * $fmtSize;
    $self->{fmtlist} = [ map { s/\x00.*$//s; $_ } unpack( ( 'A' . $fmtSize ) x $nv, $_ ) ];
    read $self->{fh}, $_, $nv * 33;
    $self->{lbllist} = [ map { s/\x00.*$//s; $_ } unpack( 'A33' x $nv, $_ ) ];
}

sub readVariableLabels($) {
    my $self = shift;
    my $nv   = $self->{nvar};
    local $_;
    read $self->{fh}, $_, $nv * 81;
    $self->{variableLabelList} = [ map { s/\x00.*$//s; $_ } unpack( 'A81' x $nv, $_ ) ];
}

sub discardExpansionFields($) {
    my $self = shift;
    local $_;
    my $size = -1;
    while ($size) {
        read $self->{fh}, $_, 5;
        $size = unpack( $self->{byteorder} == 2 ? 'V' : 'N', substr( $_, 1, 4 ) );
        read $self->{fh}, $_, $size if $size > 0;
    }
}

sub prepareDataReader($) {
    my $self = shift;
    $self->{nextRow}    = 1;
    $self->{rowPattern} = '';
    $self->{rowSize}    = 0;
    for my $vt ( @{ $self->{typlist} } ) {
        if ( $vt == 255 ) {
            $self->{rowSize} += 8;
            $self->{rowPattern} .= $self->{byteorder} == $File::Stata::DtaReader::byteOrder ? 'd' : 'A8';
        }
        elsif ( $vt == 254 ) {
            $self->{rowSize} += 4;
            $self->{rowPattern} .= $self->{byteorder} == $File::Stata::DtaReader::byteOrder ? 'f' : 'A4';
        }
        elsif ( $vt == 253 ) {
            $self->{rowSize} += 4;
            $self->{rowPattern} .= $self->{byteorder} == 2 ? 'V' : 'N';
        }
        elsif ( $vt == 252 ) {
            $self->{rowSize} += 2;
            $self->{rowPattern} .= $self->{byteorder} == 2 ? 'v' : 'n';
        }
        elsif ( $vt == 251 ) {
            $self->{rowSize} += 1;
            $self->{rowPattern} .= 'C';
        }
        elsif ( $vt < 245 ) {
            $self->{rowSize} += $vt;
            $self->{rowPattern} .= 'A' . $vt;
        }
    }
}

sub hasNext($) {
    my $self = shift;
    return $self->{nextRow} > $self->{nobs} ? undef: $self->{nextRow};
}

sub readRow($) {
    my $self = shift;
    local $_;
    return () unless $self->{rowSize} == read $self->{fh}, $_, $self->{rowSize};
    $self->{nextRow}++;
    my @a = unpack( $self->{rowPattern}, $_ );
    for ( my $i = 0 ; $i < @a ; $i++ ) {
        my $t = $self->{typlist}->[$i];
        if ( $self->{byteorder} != $File::Stata::DtaReader::byteOrder ) {
            if ( $t == 254 ) {
                $a[$i] = unpack( 'f', pack( 'N', ( unpack( 'V', $a[$i] ) ) ) );
            }
            elsif ( $t == 255 ) {
                $a[$i] = unpack( 'd', pack( 'NN', reverse( unpack( 'VV', $a[$i] ) ) ) );
            }
        }
        if ( defined $a[$i] ) {
            if ( $t < 245 ) {
                $a[$i] =~ s/\x00.*$//s;
            }
            elsif ( $t == 251 ) {
                undef $a[$i] if $a[$i] > 100 && $a[$i] < 128;
            }
            elsif ( $t == 252 ) {
                undef $a[$i] if $a[$i] > 32740 && $a[$i] < 32768;
            }
            elsif ( $t == 253 ) {
                undef $a[$i] if $a[$i] > 2147483620 && $a[$i] < 2147483648;
            }
            elsif ( $t == 254 ) {
                undef $a[$i] if $a[$i] > 1.701e38 || $a[$i] < -1.701e38;
            }
            elsif ( $t == 255 ) {
                undef $a[$i] if $a[$i] > 8.988e307 || $a[$i] < -1.798e308;
            }
        }
    }
    return @a;
}

1;
