use Test::Simple tests => 1;

use File::Stata::DtaReader;

ok($File::Stata::DtaReader::byteOrder == 1 || $File::Stata::DtaReader::byteOrder == 2);
