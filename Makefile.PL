use ExtUtils::MakeMaker;
WriteMakefile(
    NAME         => "File::Stata::DtaReader",
    VERSION_FROM => 'lib/File/Stata/DtaReader.pm',
    'dist'       =>
      { COMPRESS => 'gzip -9f', SUFFIX => 'gz', DIST_DEFAULT => 'all tardist' },
    MAN3PODS  => {},
    EXE_FILES => [qw(dta2csv)],
    LICENSE   => 'perl'
);
