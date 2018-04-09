IMPORT Std;

// Source
// https://amplab.cs.berkeley.edu/benchmark/

// ###########################################################################
// Usage
// ###########################################################################
// 1. Check pre-requisites
// 2. Create cluster (1 slave or 5 slaves)
// 3. ETL data:
//  a. Submit "extract_data()" only
//  b. Submit "transform_data()" only
//  c. Submit "load_data()" only
// 4. Check data (submit "see_data()" and/or submit Query1/2/3.check() to make sure results match expected values)
// 5. Submit Query1/2/3 A, B and C queries.


// ###########################################################################
// I. Pre-requisites
// ###########################################################################
// 1. Install awscli: sudo apt-get install awscli
// 2. Configure aws under "hpcc" user (i.e. must have region and access/secret key configured under "hpcc" user, no IAM policies needed)
// 3. mydropzone basically hosts twice (2x) the size of the data to ETL (the parts, and the concatenated version to spray)
// 4. hthor is on the node with mydropzone
// 5. Number of slaves will dictate the DATASET_SIZE variable below (i.e. 5 slaves = '5nodes', 1 slave = '1node')
// 6. The benchmark says it used m2.4xlarge instances. The new equivalent is r4 type EC2 instance which has better I/O performance, more RAM, better memory bandwidth and enhanced networking.



// Possible values: tiny,1node,5nodes
DATASET_SIZE := '5nodes';


//oDropZoneIP := '192.168.99.101';
oDropZoneIP := '192.168.5.10';
oBaseDirectory := '/var/lib/HPCCSystems/mydropzone/amplab/' + DATASET_SIZE + '/';

oCrawlDir := oBaseDirectory + 'crawl/';
oRankingsDir := oBaseDirectory + 'rankings/';
oUserVisitsDir := oBaseDirectory + 'uservisits/';

oBaseS3URN := 's3://big-data-benchmark/pavlo/text/' + DATASET_SIZE + '/';
oCrawlURN := oBaseS3URN + 'crawl/';
oRankingsURN := oBaseS3URN + 'rankings/';
oUserVisitsURN := oBaseS3URN + 'uservisits/';

line_layout := RECORD
	STRING line;
	STRING2 nl := '\r\n';
END;

spray( STRING pPhysicalFilename, STRING pFieldSeparator, STRING pLogicalFilename ) :=
	Std.File.SprayDelimited( 
		sourceIP := oDropZoneIP, 
		sourcePath := pPhysicalFilename,
		//sourceCsvSeparate:='\\,',
		sourceCsvSeparate := pFieldSeparator,
		destinationGroup:='mythor',
		destinationLogicalName := pLogicalFilename,
		allowOverwrite:=TRUE );

mkdir( STRING localUri , BOOLEAN makeParents = false) := FUNCTION
	A := 'bash -c "[ ! -d ' + localUri + ' ] && mkdir ';
	B := A + IF(makeParents, '-p ', '');
	C := B + localUri + ' || exit 0"';
	Cmd := C;
	RETURN PIPE(Cmd + localUri, line_layout, CSV(SEPARATOR(''), QUOTE('')) );
END;
aws_s3_cp( STRING pS3URN, STRING pDest ) := PIPE('/usr/bin/aws s3 cp --quiet --recursive ' + pS3URN + ' ' + pDest, line_layout );
concat_files( STRING pSources, STRING pFinalFinalName ) := PIPE('/bin/bash -c "/bin/cat ' + pSources + ' > ' + pFinalFinalName + '"', line_layout );


extract_data() := FUNCTION
	RETURN SEQUENTIAL(
		#OPTION('targetClusterType','hthor'),
		OUTPUT( mkdir( oBaseDirectory, true ), NAMED( 'MkdirBaseDir' )),
		OUTPUT( mkdir( oCrawlDir, false ), NAMED( 'MkdirCrawlDir' )),
		OUTPUT( mkdir( oRankingsDir, false ), NAMED( 'MkdirRankingsDir' )),
		OUTPUT( mkdir( oUserVisitsDir, false ), NAMED( 'MkdirUserVisitsDir' )),
		//OUTPUT( aws_s3_cp( oCrawlURN, oCrawlDir ), NAMED( 'S3CpCrawl' )),
		OUTPUT( aws_s3_cp( oRankingsURN, oRankingsDir ), NAMED( 'S3CpRankings' )),
		OUTPUT( aws_s3_cp( oUserVisitsURN, oUserVisitsDir ), NAMED( 'S3CpUserVisits' ))
	);
END;

transform_data() := FUNCTION
	RETURN SEQUENTIAL(
		#OPTION('targetClusterType','hthor'),
		//OUTPUT( concat_files( oCrawlDir + 'part-*', oCrawlDir + 'all-parts' ), NAMED('ConcatCrawlFiles') ),
		OUTPUT( concat_files( oRankingsDir + 'part-*', oRankingsDir + 'all-parts' ), NAMED('ConcatRankingsFiles') ),
		OUTPUT( concat_files( oUserVisitsDir + 'part-*', oUserVisitsDir + 'all-parts' ), NAMED('ConcatUserVisitsFiles') )		
	);
END;

load_data() := FUNCTION
	RETURN SEQUENTIAL(
		//spray( oCrawlDir + 'all-parts', '\\,', '~amplab::benchmark::crawl::raw' ),
		spray( oRankingsDir + 'all-parts', '\\,', '~amplab::benchmark::rankings::raw' ),
		spray( oUserVisitsDir + 'all-parts', '\\,', '~amplab::benchmark::uservisits::raw' )		
	);
END;



// ###########################################################################
// II. Loading data
// ###########################################################################

// 1. Extract data
extract_data();
// 2. Transform data
//transform_data();
// 3. Load data
//load_data();

ranking_layout := RECORD
	STRING pageURL;
	INTEGER pageRank;
	INTEGER avgDuration;
END;

uservisit_layout := RECORD
	STRING sourceIP;
	STRING destURL;
	STRING visitDate;
	REAL8 adRevenue;
	STRING userAgent;
	STRING countryCode;
	STRING languageCode;
	STRING searchWord;
	INTEGER duration;
END;

oRankingsDS := DATASET( '~amplab::benchmark::rankings::raw', ranking_layout, CSV );
oUserVisitsDS := DATASET( '~amplab::benchmark::uservisits::raw', uservisit_layout, CSV );


// ###########################################################################
// III. Check Data
// ###########################################################################
see_data() := FUNCTION
	RETURN SEQUENTIAL(
		OUTPUT( oRankingsDS, NAMED('RankingsData') ),
		OUTPUT( oUserVisitsDS, NAMED('UserVisitsData') )
	);
END;

// ###########################################################################
// IV. Queries
// ###########################################################################

Query1 := MODULE

	SHARED q1( INTEGER X ) := TABLE( oRankingsDS( pageRank > X ), { pageURL, pageRank } );
	
	EXPORT q1A := q1( 1000 );
	EXPORT q1B := q1( 100 );
	EXPORT q1C := q1( 10 );
	
	EXPORT A := SEQUENTIAL( OUTPUT( q1A,, '~amplab::benchmark::query1::a', OVERWRITE, NAMED('Query1A') ), OUTPUT('Dummy') );
	
	EXPORT B := SEQUENTIAL( OUTPUT( q1B,, '~amplab::benchmark::query1::b', OVERWRITE, NAMED('Query1B') ), OUTPUT('Dummy')  );
		
	EXPORT C := SEQUENTIAL( OUTPUT( q1C,, '~amplab::benchmark::query1::c', OVERWRITE, NAMED('Query1C') ), OUTPUT('Dummy')  );
		
	EXPORT check() := SEQUENTIAL(
		OUTPUT( TABLE( q1A, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 32888; }), NAMED('Query1ACount') ),
		OUTPUT( TABLE( q1B, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 3331851; }), NAMED('Query1BCount') ),
		OUTPUT( TABLE( q1C, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 89974976; }), NAMED('Query1CCount') )
	);
	
END;


Query2 := MODULE

	SHARED q2Opt( INTEGER X ) := TABLE( oUserVisitsDS, { STRING subIP := sourceIP[1..X]; REAL8 sum_adRevenue := SUM(GROUP, adRevenue); }, sourceIP[1..X], MANY, MERGE, UNSORTED );
	SHARED q2Orig( INTEGER X ) := TABLE( oUserVisitsDS, { STRING subIP := sourceIP[1..X]; REAL8 sum_adRevenue := SUM(GROUP, adRevenue); }, sourceIP[1..X] );
	SHARED q2( INTEGER X ) := q2Orig( X );
	SHARED q2A := q2( 8 );
	SHARED q2B := q2( 10 );
	SHARED q2C := q2( 12 );
	
	EXPORT A := SEQUENTIAL( OUTPUT( q2A,, '~amplab::benchmark::query2::a', OVERWRITE, NAMED('Query2A') ), OUTPUT('Dummy') );
	
	EXPORT B := SEQUENTIAL( OUTPUT( q2B,, '~amplab::benchmark::query2::b', OVERWRITE, NAMED('Query2B') ), OUTPUT('Dummy')  );
		
	EXPORT C := SEQUENTIAL( OUTPUT( q2C,, '~amplab::benchmark::query2::c', OVERWRITE, NAMED('Query2C') ), OUTPUT('Dummy')  );
		
	EXPORT check() := SEQUENTIAL(
		OUTPUT( TABLE( q2A, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 2067313; }), NAMED('Query2ACount') ),
		OUTPUT( TABLE( q2B, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 31348913; }), NAMED('Query2BCount') ),
		OUTPUT( TABLE( q2C, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 253890330; }), NAMED('Query2CCount') )
	);

END;

Query3 := MODULE
	SHARED q3( STRING pEndDate ) := FUNCTION
		FilteredUserVisits := oUserVisitsDS( visitDate BETWEEN '1980-01-01' AND pEndDate );
		DistributedFilteredVisits := DISTRIBUTE( FilteredUserVisits, HASH32(destURL) );
		DistributedRankings := DISTRIBUTE( oRankingsDS, HASH32(pageURL) );
		RankingsWithVisits := JOIN( DistributedRankings, DistributedFilteredVisits, LEFT.pageURL = RIGHT.destURL, LOCAL );
		DistributedRankingsWithVisits := DISTRIBUTE( RankingsWithVisits, HASH32(sourceIP) );
		Aggregates := TABLE( DistributedRankingsWithVisits, { sourceIP; REAL8 avgPageRank := AVE( GROUP, pageRank ); REAL8 totalRevenue := SUM( GROUP, adRevenue ); }, sourceIP, LOCAL, MERGE );
		RETURN SORT(Aggregates, totalRevenue );
	END;
	
	SHARED q3A := q3( '1980-04-01' );
	SHARED q3B := q3( '1983-01-01' );
	SHARED q3C := q3( '2010-01-01' );
	
	EXPORT A := SEQUENTIAL( OUTPUT( CHOOSEN(q3A, 1),, '~amplab::benchmark::query3::a', OVERWRITE, NAMED('Query3A') ), OUTPUT('Dummy') );
	
	EXPORT B := SEQUENTIAL( OUTPUT( CHOOSEN(q3B, 1),, '~amplab::benchmark::query3::b', OVERWRITE, NAMED('Query3B') ), OUTPUT('Dummy')  );
		
	EXPORT C := SEQUENTIAL( OUTPUT( CHOOSEN(q3C, 1),, '~amplab::benchmark::query3::c', OVERWRITE, NAMED('Query3C') ), OUTPUT('Dummy')  );
		
	EXPORT check() := SEQUENTIAL(
		OUTPUT( TABLE( q3A, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 485312; }), NAMED('Query3ACount') ),
		OUTPUT( TABLE( q3B, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 53332015; }), NAMED('Query3BCount') ),
		OUTPUT( TABLE( q3C, { UNSIGNED actual := COUNT( GROUP ); UNSIGNED expected := 533287121; }), NAMED('Query3CCount') )
	);

END;

query3Opt() := MACRO
	oUserVisitsDistDS := DISTRIBUTE( oUserVisitsDS, HASH32( destURL ) );
	oRankingsDistDS := DISTRIBUTE( oRankingsDS, HASH32( pageURL ) );
	q3( STRING pEndDate ) := FUNCTION
		FilteredUserVisits := oUserVisitsDistDS( visitDate BETWEEN '1980-01-01' AND pEndDate );
		RankingsWithUserVisits := JOIN( oRankingsDistDS, FilteredUserVisits, LEFT.pageURL = RIGHT.destURL, LOCAL);
		Aggregates := TABLE( RankingsWithUserVisits, { sourceIP; REAL8 avgPageRank := AVE( GROUP, pageRank ); REAL8 totalRevenue := SUM( GROUP, adRevenue ); }, sourceIP, LOCAL, MERGE );
		RETURN SORT(Aggregates, totalRevenue );
	END;
	// 3A = 485,312 rows
	q3_A := q3( '1980-04-01' );
	// 3B = 53,332,015 rows
	q3_B := q3( '1983-01-01' );
	// 3C = 533,287,121 rows
	q3_C := q3( '2010-01-01' );
	OUTPUT( q3_A, NAMED('Query3AOpt') );
	OUTPUT( COUNT(q3_A), NAMED('Query3AOptCount') );
	OUTPUT( q3_B, NAMED('Query3BOpt') );
	OUTPUT( COUNT(q3_B), NAMED('Query3BOptCount') );
	OUTPUT( q3_C, NAMED('Query3COpt') );
	OUTPUT( COUNT(q3_C), NAMED('Query3COptCount') );
ENDMACRO;

//Query1.check();

/*
Query1.A;
Query1.B;
Query1.C;
*/


//Query2.check();

/*
Query2.A;
Query2.B;
Query2.C;
*/


//Query3.check();

/*
Query3.A;
Query3.B;
Query3.C;
*/
