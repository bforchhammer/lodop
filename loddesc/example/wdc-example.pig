-- 2012-06-16 hannes
-- change $prefix to your loddesc folder path for this to run!
-- or: command line: ./pig -x local -param prefix=/path/to/loddesc ~/path/to/loddesc/example/wdc-example.pig

%default prefix '/home/hannes/git/loddesc'

register  '$prefix/core/target/loddesc-core-0.1-with-dependencies.jar';
quads = load '$prefix/example/wdc-example.nq'
    using de.wbsg.loddesc.importer.QuadLoader()
    AS (subject,predicate,object:tuple(ntype:int,value,dtlang),graph);

-- top properties by number of triples
--preds = group quads by predicate;
--counts = foreach preds generate group, COUNT(quads) as cnt;
--topProperties = order counts by cnt DESC;
--STORE topProperties INTO '$prefix/example/topPropertiesTriples' USING PigStorage(',');


-- top classes by number of entities and sorted
--typedstuff = filter quads by predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
--classes = group typedstuff by object.value;
--classCounts = foreach classes generate group, COUNT(typedstuff) as cnt;
--topClasses = order classCounts by cnt DESC;
--STORE topClasses INTO '$prefix/example/topClassesEntities' USING PigStorage(',');


-- top classes by number of plds and sorted

-- filter all type statements
--typedstuff = filter quads by predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

-- generate pay-level-domain and filter out redundant tuples
--classesAndDomainsA = FOREACH typedstuff GENERATE object.value AS class,de.wbsg.loddesc.functions.PayLevelDomain(graph) AS domain;
--classesAndDomains = DISTINCT classesAndDomainsA;

-- count the number of domains the class appeared on
--classes = GROUP classesAndDomains by class;
--classCounts = foreach classes generate group, COUNT(classesAndDomains) as cnt;

-- sort
--topClasses = order classCounts by cnt DESC;
--STORE topClasses INTO '$prefix/example/topClassesDomains' USING PigStorage(',');

--dump topClasses;




-- top classes by number of pdls, by tld and sorted

-- filter all type statements
--typedstuff = FILTER quads BY predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';

-- generate pay-level-domain and filter out redundant tuples
--classesAndDomainsA = FOREACH typedstuff GENERATE object.value AS type,de.wbsg.loddesc.functions.PayLevelDomain(graph) AS domain;
--classesAndDomains = DISTINCT classesAndDomainsA;

--tlds = GROUP classesAndDomains by (de.wbsg.loddesc.functions.TopLevelDomain(domain),type);
--tldTypeCounts = FOREACH tlds GENERATE FLATTEN(group) AS (tld,type), COUNT(classesAndDomains) as cnt;

--topClassesTld = ORDER tldTypeCounts by tld, cnt DESC;
--dump topClassesTld;


--predicateVocabs = FOREACH quads GENERATE de.wbsg.loddesc.functions.Vocab(predicate) AS vocab, subject;
--typedstuff = FILTER quads BY predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
--objectVocabs = FOREACH typedstuff GENERATE de.wbsg.loddesc.functions.Vocab(object.value) AS vocab, subject;
--allVocabsEntities = UNION predicateVocabs, objectVocabs;
--vocabsEntities = DISTINCT allVocabsEntities;
--vocabs = GROUP vocabsEntities by vocab;
--vocabCounts = FOREACH vocabs GENERATE group, COUNT(vocabsEntities) as cnt;
--topVocabsEntities = ORDER vocabCounts BY cnt DESC;
--DUMP    topVocabsEntities;


-- cooc by # entities
subjGroups = GROUP quads BY subject;
subjGroupsCopy = FOREACH subjGroups GENERATE FLATTEN(quads.predicate) AS p1, FLATTEN(quads.predicate) AS p2;
propertyCooc = FILTER subjGroupsCopy BY p1 < p2;
propertyCoocGroup = GROUP propertyCooc by *;
propertyCoocCounts = FOREACH propertyCoocGroup GENERATE group, COUNT(propertyCooc) as cnt;

--propertyCoocCountsGrouped = GROUP propertyCoocCounts BY group.p1;

propertyCoocSorted = ORDER propertyCoocCounts BY cnt DESC;
propertyCoocSortedTop = FILTER propertyCoocSorted BY cnt > 1;

DUMP propertyCoocSortedTop;






