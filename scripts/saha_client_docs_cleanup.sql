/************************************************** 
 * VSTS 46193
 * Script to delete any inappropriate files in 
 * Client Documents before NGO Portal Deployment
 * Vrs    Date        By          Description
 * 1.0    06/01/2020  P Le Leu    Initial Creation
 ***************************************************/
PROMPT Starting VSTS 46193 
PROMPT Deleting Individual Client Documents

DELETE FROM client_document_associations WHERE cda_cdo_reference = 2; DELETE FROM client_documents WHERE cdo_reference = 2;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 1584; DELETE FROM client_documents WHERE cdo_reference = 1584;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 1591; DELETE FROM client_documents WHERE cdo_reference = 1591;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 2359; DELETE FROM client_documents WHERE cdo_reference = 2359;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 2545; DELETE FROM client_documents WHERE cdo_reference = 2545;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 2585; DELETE FROM client_documents WHERE cdo_reference = 2585;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 2642; DELETE FROM client_documents WHERE cdo_reference = 2642;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 2586; DELETE FROM client_documents WHERE cdo_reference = 2586;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 4213; DELETE FROM client_documents WHERE cdo_reference = 4213;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 4159; DELETE FROM client_documents WHERE cdo_reference = 4159;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 6738; DELETE FROM client_documents WHERE cdo_reference = 6738;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 5121; DELETE FROM client_documents WHERE cdo_reference = 5121;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 5088; DELETE FROM client_documents WHERE cdo_reference = 5088;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 4510; DELETE FROM client_documents WHERE cdo_reference = 4510;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 7223; DELETE FROM client_documents WHERE cdo_reference = 7223;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 13039; DELETE FROM client_documents WHERE cdo_reference = 13039;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 10358; DELETE FROM client_documents WHERE cdo_reference = 10358;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 13051; DELETE FROM client_documents WHERE cdo_reference = 13051;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 13052; DELETE FROM client_documents WHERE cdo_reference = 13052;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 13114; DELETE FROM client_documents WHERE cdo_reference = 13114;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 13115; DELETE FROM client_documents WHERE cdo_reference = 13115;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 13116; DELETE FROM client_documents WHERE cdo_reference = 13116;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 10379; DELETE FROM client_documents WHERE cdo_reference = 10379;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 10380; DELETE FROM client_documents WHERE cdo_reference = 10380;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 12322; DELETE FROM client_documents WHERE cdo_reference = 12322;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 10378; DELETE FROM client_documents WHERE cdo_reference = 10378;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 17274; DELETE FROM client_documents WHERE cdo_reference = 17274;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 17311; DELETE FROM client_documents WHERE cdo_reference = 17311;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 17313; DELETE FROM client_documents WHERE cdo_reference = 17313;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 17315; DELETE FROM client_documents WHERE cdo_reference = 17315;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 17316; DELETE FROM client_documents WHERE cdo_reference = 17316;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 17319; DELETE FROM client_documents WHERE cdo_reference = 17319;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 17314; DELETE FROM client_documents WHERE cdo_reference = 17314;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 17502; DELETE FROM client_documents WHERE cdo_reference = 17502;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 21291; DELETE FROM client_documents WHERE cdo_reference = 21291;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 21082; DELETE FROM client_documents WHERE cdo_reference = 21082;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 21275; DELETE FROM client_documents WHERE cdo_reference = 21275;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 21451; DELETE FROM client_documents WHERE cdo_reference = 21451;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 21463; DELETE FROM client_documents WHERE cdo_reference = 21463;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 19378; DELETE FROM client_documents WHERE cdo_reference = 19378;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 26447; DELETE FROM client_documents WHERE cdo_reference = 26447;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 26448; DELETE FROM client_documents WHERE cdo_reference = 26448;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 26029; DELETE FROM client_documents WHERE cdo_reference = 26029;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 26161; DELETE FROM client_documents WHERE cdo_reference = 26161;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 24350; DELETE FROM client_documents WHERE cdo_reference = 24350;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 26772; DELETE FROM client_documents WHERE cdo_reference = 26772;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 29573; DELETE FROM client_documents WHERE cdo_reference = 29573;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 29680; DELETE FROM client_documents WHERE cdo_reference = 29680;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 29580; DELETE FROM client_documents WHERE cdo_reference = 29580;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 29575; DELETE FROM client_documents WHERE cdo_reference = 29575;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 31303; DELETE FROM client_documents WHERE cdo_reference = 31303;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 31302; DELETE FROM client_documents WHERE cdo_reference = 31302;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 36534; DELETE FROM client_documents WHERE cdo_reference = 36534;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 36806; DELETE FROM client_documents WHERE cdo_reference = 36806;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 38225; DELETE FROM client_documents WHERE cdo_reference = 38225;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 36596; DELETE FROM client_documents WHERE cdo_reference = 36596;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 36877; DELETE FROM client_documents WHERE cdo_reference = 36877;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 42852; DELETE FROM client_documents WHERE cdo_reference = 42852;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 42640; DELETE FROM client_documents WHERE cdo_reference = 42640;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 43375; DELETE FROM client_documents WHERE cdo_reference = 43375;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 43359; DELETE FROM client_documents WHERE cdo_reference = 43359;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 43365; DELETE FROM client_documents WHERE cdo_reference = 43365;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 42556; DELETE FROM client_documents WHERE cdo_reference = 42556;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 44185; DELETE FROM client_documents WHERE cdo_reference = 44185;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 44040; DELETE FROM client_documents WHERE cdo_reference = 44040;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 45606; DELETE FROM client_documents WHERE cdo_reference = 45606;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 44140; DELETE FROM client_documents WHERE cdo_reference = 44140;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 44637; DELETE FROM client_documents WHERE cdo_reference = 44637;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 44708; DELETE FROM client_documents WHERE cdo_reference = 44708;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 44748; DELETE FROM client_documents WHERE cdo_reference = 44748;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 45071; DELETE FROM client_documents WHERE cdo_reference = 45071;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 46759; DELETE FROM client_documents WHERE cdo_reference = 46759;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 48246; DELETE FROM client_documents WHERE cdo_reference = 48246;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 48248; DELETE FROM client_documents WHERE cdo_reference = 48248;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 48251; DELETE FROM client_documents WHERE cdo_reference = 48251;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 48247; DELETE FROM client_documents WHERE cdo_reference = 48247;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 48249; DELETE FROM client_documents WHERE cdo_reference = 48249;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54222; DELETE FROM client_documents WHERE cdo_reference = 54222;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 53340; DELETE FROM client_documents WHERE cdo_reference = 53340;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54087; DELETE FROM client_documents WHERE cdo_reference = 54087;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54596; DELETE FROM client_documents WHERE cdo_reference = 54596;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54475; DELETE FROM client_documents WHERE cdo_reference = 54475;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54805; DELETE FROM client_documents WHERE cdo_reference = 54805;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54133; DELETE FROM client_documents WHERE cdo_reference = 54133;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 53337; DELETE FROM client_documents WHERE cdo_reference = 53337;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54813; DELETE FROM client_documents WHERE cdo_reference = 54813;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 53260; DELETE FROM client_documents WHERE cdo_reference = 53260;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54221; DELETE FROM client_documents WHERE cdo_reference = 54221;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54527; DELETE FROM client_documents WHERE cdo_reference = 54527;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54115; DELETE FROM client_documents WHERE cdo_reference = 54115;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54132; DELETE FROM client_documents WHERE cdo_reference = 54132;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 53322; DELETE FROM client_documents WHERE cdo_reference = 53322;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54463; DELETE FROM client_documents WHERE cdo_reference = 54463;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54522; DELETE FROM client_documents WHERE cdo_reference = 54522;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 54542; DELETE FROM client_documents WHERE cdo_reference = 54542;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59761; DELETE FROM client_documents WHERE cdo_reference = 59761;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61013; DELETE FROM client_documents WHERE cdo_reference = 61013;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60967; DELETE FROM client_documents WHERE cdo_reference = 60967;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61017; DELETE FROM client_documents WHERE cdo_reference = 61017;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59762; DELETE FROM client_documents WHERE cdo_reference = 59762;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59725; DELETE FROM client_documents WHERE cdo_reference = 59725;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59764; DELETE FROM client_documents WHERE cdo_reference = 59764;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59766; DELETE FROM client_documents WHERE cdo_reference = 59766;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59765; DELETE FROM client_documents WHERE cdo_reference = 59765;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60491; DELETE FROM client_documents WHERE cdo_reference = 60491;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60089; DELETE FROM client_documents WHERE cdo_reference = 60089;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59317; DELETE FROM client_documents WHERE cdo_reference = 59317;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59696; DELETE FROM client_documents WHERE cdo_reference = 59696;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59772; DELETE FROM client_documents WHERE cdo_reference = 59772;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59760; DELETE FROM client_documents WHERE cdo_reference = 59760;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60068; DELETE FROM client_documents WHERE cdo_reference = 60068;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60915; DELETE FROM client_documents WHERE cdo_reference = 60915;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60922; DELETE FROM client_documents WHERE cdo_reference = 60922;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60490; DELETE FROM client_documents WHERE cdo_reference = 60490;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60493; DELETE FROM client_documents WHERE cdo_reference = 60493;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60494; DELETE FROM client_documents WHERE cdo_reference = 60494;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60495; DELETE FROM client_documents WHERE cdo_reference = 60495;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60500; DELETE FROM client_documents WHERE cdo_reference = 60500;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60938; DELETE FROM client_documents WHERE cdo_reference = 60938;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 58474; DELETE FROM client_documents WHERE cdo_reference = 58474;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60599; DELETE FROM client_documents WHERE cdo_reference = 60599;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59745; DELETE FROM client_documents WHERE cdo_reference = 59745;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59310; DELETE FROM client_documents WHERE cdo_reference = 59310;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59305; DELETE FROM client_documents WHERE cdo_reference = 59305;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60483; DELETE FROM client_documents WHERE cdo_reference = 60483;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60472; DELETE FROM client_documents WHERE cdo_reference = 60472;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 60601; DELETE FROM client_documents WHERE cdo_reference = 60601;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59722; DELETE FROM client_documents WHERE cdo_reference = 59722;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 59767; DELETE FROM client_documents WHERE cdo_reference = 59767;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 58847; DELETE FROM client_documents WHERE cdo_reference = 58847;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 58908; DELETE FROM client_documents WHERE cdo_reference = 58908;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 58451; DELETE FROM client_documents WHERE cdo_reference = 58451;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 58432; DELETE FROM client_documents WHERE cdo_reference = 58432;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 64297; DELETE FROM client_documents WHERE cdo_reference = 64297;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 62130; DELETE FROM client_documents WHERE cdo_reference = 62130;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 64353; DELETE FROM client_documents WHERE cdo_reference = 64353;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61331; DELETE FROM client_documents WHERE cdo_reference = 61331;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61683; DELETE FROM client_documents WHERE cdo_reference = 61683;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63273; DELETE FROM client_documents WHERE cdo_reference = 63273;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63749; DELETE FROM client_documents WHERE cdo_reference = 63749;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61326; DELETE FROM client_documents WHERE cdo_reference = 61326;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 62160; DELETE FROM client_documents WHERE cdo_reference = 62160;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63305; DELETE FROM client_documents WHERE cdo_reference = 63305;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 64363; DELETE FROM client_documents WHERE cdo_reference = 64363;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 64010; DELETE FROM client_documents WHERE cdo_reference = 64010;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61757; DELETE FROM client_documents WHERE cdo_reference = 61757;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65152; DELETE FROM client_documents WHERE cdo_reference = 65152;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65175; DELETE FROM client_documents WHERE cdo_reference = 65175;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63268; DELETE FROM client_documents WHERE cdo_reference = 63268;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65197; DELETE FROM client_documents WHERE cdo_reference = 65197;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65212; DELETE FROM client_documents WHERE cdo_reference = 65212;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63329; DELETE FROM client_documents WHERE cdo_reference = 63329;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65247; DELETE FROM client_documents WHERE cdo_reference = 65247;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61355; DELETE FROM client_documents WHERE cdo_reference = 61355;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63987; DELETE FROM client_documents WHERE cdo_reference = 63987;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63225; DELETE FROM client_documents WHERE cdo_reference = 63225;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61332; DELETE FROM client_documents WHERE cdo_reference = 61332;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63286; DELETE FROM client_documents WHERE cdo_reference = 63286;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 64323; DELETE FROM client_documents WHERE cdo_reference = 64323;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61336; DELETE FROM client_documents WHERE cdo_reference = 61336;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 62076; DELETE FROM client_documents WHERE cdo_reference = 62076;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 62099; DELETE FROM client_documents WHERE cdo_reference = 62099;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61356; DELETE FROM client_documents WHERE cdo_reference = 61356;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61758; DELETE FROM client_documents WHERE cdo_reference = 61758;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61668; DELETE FROM client_documents WHERE cdo_reference = 61668;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63671; DELETE FROM client_documents WHERE cdo_reference = 63671;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63704; DELETE FROM client_documents WHERE cdo_reference = 63704;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63269; DELETE FROM client_documents WHERE cdo_reference = 63269;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63270; DELETE FROM client_documents WHERE cdo_reference = 63270;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61473; DELETE FROM client_documents WHERE cdo_reference = 61473;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 62041; DELETE FROM client_documents WHERE cdo_reference = 62041;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 62185; DELETE FROM client_documents WHERE cdo_reference = 62185;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63655; DELETE FROM client_documents WHERE cdo_reference = 63655;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 64009; DELETE FROM client_documents WHERE cdo_reference = 64009;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61357; DELETE FROM client_documents WHERE cdo_reference = 61357;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63214; DELETE FROM client_documents WHERE cdo_reference = 63214;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63985; DELETE FROM client_documents WHERE cdo_reference = 63985;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63996; DELETE FROM client_documents WHERE cdo_reference = 63996;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 64037; DELETE FROM client_documents WHERE cdo_reference = 64037;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63271; DELETE FROM client_documents WHERE cdo_reference = 63271;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 63313; DELETE FROM client_documents WHERE cdo_reference = 63313;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61370; DELETE FROM client_documents WHERE cdo_reference = 61370;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61708; DELETE FROM client_documents WHERE cdo_reference = 61708;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61353; DELETE FROM client_documents WHERE cdo_reference = 61353;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61358; DELETE FROM client_documents WHERE cdo_reference = 61358;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 61458; DELETE FROM client_documents WHERE cdo_reference = 61458;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 69090; DELETE FROM client_documents WHERE cdo_reference = 69090;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 68368; DELETE FROM client_documents WHERE cdo_reference = 68368;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 68363; DELETE FROM client_documents WHERE cdo_reference = 68363;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 68364; DELETE FROM client_documents WHERE cdo_reference = 68364;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 69184; DELETE FROM client_documents WHERE cdo_reference = 69184;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65897; DELETE FROM client_documents WHERE cdo_reference = 65897;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66848; DELETE FROM client_documents WHERE cdo_reference = 66848;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66871; DELETE FROM client_documents WHERE cdo_reference = 66871;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65904; DELETE FROM client_documents WHERE cdo_reference = 65904;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66039; DELETE FROM client_documents WHERE cdo_reference = 66039;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 68380; DELETE FROM client_documents WHERE cdo_reference = 68380;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 67306; DELETE FROM client_documents WHERE cdo_reference = 67306;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65574; DELETE FROM client_documents WHERE cdo_reference = 65574;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66282; DELETE FROM client_documents WHERE cdo_reference = 66282;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66294; DELETE FROM client_documents WHERE cdo_reference = 66294;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66280; DELETE FROM client_documents WHERE cdo_reference = 66280;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65875; DELETE FROM client_documents WHERE cdo_reference = 65875;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66041; DELETE FROM client_documents WHERE cdo_reference = 66041;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 69526; DELETE FROM client_documents WHERE cdo_reference = 69526;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 69538; DELETE FROM client_documents WHERE cdo_reference = 69538;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 69461; DELETE FROM client_documents WHERE cdo_reference = 69461;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 69460; DELETE FROM client_documents WHERE cdo_reference = 69460;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 68360; DELETE FROM client_documents WHERE cdo_reference = 68360;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 68362; DELETE FROM client_documents WHERE cdo_reference = 68362;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 69510; DELETE FROM client_documents WHERE cdo_reference = 69510;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 69194; DELETE FROM client_documents WHERE cdo_reference = 69194;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65561; DELETE FROM client_documents WHERE cdo_reference = 65561;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65905; DELETE FROM client_documents WHERE cdo_reference = 65905;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65698; DELETE FROM client_documents WHERE cdo_reference = 65698;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65877; DELETE FROM client_documents WHERE cdo_reference = 65877;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65930; DELETE FROM client_documents WHERE cdo_reference = 65930;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66037; DELETE FROM client_documents WHERE cdo_reference = 66037;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 65962; DELETE FROM client_documents WHERE cdo_reference = 65962;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 67321; DELETE FROM client_documents WHERE cdo_reference = 67321;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 67349; DELETE FROM client_documents WHERE cdo_reference = 67349;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 67353; DELETE FROM client_documents WHERE cdo_reference = 67353;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 68361; DELETE FROM client_documents WHERE cdo_reference = 68361;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 68315; DELETE FROM client_documents WHERE cdo_reference = 68315;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66267; DELETE FROM client_documents WHERE cdo_reference = 66267;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 66365; DELETE FROM client_documents WHERE cdo_reference = 66365;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 70784; DELETE FROM client_documents WHERE cdo_reference = 70784;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 70795; DELETE FROM client_documents WHERE cdo_reference = 70795;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 70241; DELETE FROM client_documents WHERE cdo_reference = 70241;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 72262; DELETE FROM client_documents WHERE cdo_reference = 72262;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 70227; DELETE FROM client_documents WHERE cdo_reference = 70227;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 70785; DELETE FROM client_documents WHERE cdo_reference = 70785;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 70170; DELETE FROM client_documents WHERE cdo_reference = 70170;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73433; DELETE FROM client_documents WHERE cdo_reference = 73433;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 70185; DELETE FROM client_documents WHERE cdo_reference = 70185;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73979; DELETE FROM client_documents WHERE cdo_reference = 73979;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 72720; DELETE FROM client_documents WHERE cdo_reference = 72720;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73357; DELETE FROM client_documents WHERE cdo_reference = 73357;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 72235; DELETE FROM client_documents WHERE cdo_reference = 72235;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73480; DELETE FROM client_documents WHERE cdo_reference = 73480;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73483; DELETE FROM client_documents WHERE cdo_reference = 73483;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73427; DELETE FROM client_documents WHERE cdo_reference = 73427;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73430; DELETE FROM client_documents WHERE cdo_reference = 73430;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73431; DELETE FROM client_documents WHERE cdo_reference = 73431;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 74064; DELETE FROM client_documents WHERE cdo_reference = 74064;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73432; DELETE FROM client_documents WHERE cdo_reference = 73432;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 74060; DELETE FROM client_documents WHERE cdo_reference = 74060;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 72829; DELETE FROM client_documents WHERE cdo_reference = 72829;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 73429; DELETE FROM client_documents WHERE cdo_reference = 73429;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 74487; DELETE FROM client_documents WHERE cdo_reference = 74487;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 75343; DELETE FROM client_documents WHERE cdo_reference = 75343;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 75291; DELETE FROM client_documents WHERE cdo_reference = 75291;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 75297; DELETE FROM client_documents WHERE cdo_reference = 75297;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 74551; DELETE FROM client_documents WHERE cdo_reference = 74551;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 75261; DELETE FROM client_documents WHERE cdo_reference = 75261;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 78083; DELETE FROM client_documents WHERE cdo_reference = 78083;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 75319; DELETE FROM client_documents WHERE cdo_reference = 75319;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 77479; DELETE FROM client_documents WHERE cdo_reference = 77479;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 76545; DELETE FROM client_documents WHERE cdo_reference = 76545;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 78098; DELETE FROM client_documents WHERE cdo_reference = 78098;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 77391; DELETE FROM client_documents WHERE cdo_reference = 77391;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 77390; DELETE FROM client_documents WHERE cdo_reference = 77390;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 78544; DELETE FROM client_documents WHERE cdo_reference = 78544;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 78563; DELETE FROM client_documents WHERE cdo_reference = 78563;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 81847; DELETE FROM client_documents WHERE cdo_reference = 81847;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 81910; DELETE FROM client_documents WHERE cdo_reference = 81910;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 82259; DELETE FROM client_documents WHERE cdo_reference = 82259;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 80812; DELETE FROM client_documents WHERE cdo_reference = 80812;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 83027; DELETE FROM client_documents WHERE cdo_reference = 83027;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 79731; DELETE FROM client_documents WHERE cdo_reference = 79731;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 79288; DELETE FROM client_documents WHERE cdo_reference = 79288;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 79225; DELETE FROM client_documents WHERE cdo_reference = 79225;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 79719; DELETE FROM client_documents WHERE cdo_reference = 79719;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 80778; DELETE FROM client_documents WHERE cdo_reference = 80778;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 80799; DELETE FROM client_documents WHERE cdo_reference = 80799;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 81293; DELETE FROM client_documents WHERE cdo_reference = 81293;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 82346; DELETE FROM client_documents WHERE cdo_reference = 82346;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 81241; DELETE FROM client_documents WHERE cdo_reference = 81241;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 79247; DELETE FROM client_documents WHERE cdo_reference = 79247;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 83075; DELETE FROM client_documents WHERE cdo_reference = 83075;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 79361; DELETE FROM client_documents WHERE cdo_reference = 79361;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 82239; DELETE FROM client_documents WHERE cdo_reference = 82239;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 81848; DELETE FROM client_documents WHERE cdo_reference = 81848;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 81917; DELETE FROM client_documents WHERE cdo_reference = 81917;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 81928; DELETE FROM client_documents WHERE cdo_reference = 81928;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 80745; DELETE FROM client_documents WHERE cdo_reference = 80745;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 79656; DELETE FROM client_documents WHERE cdo_reference = 79656;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86273; DELETE FROM client_documents WHERE cdo_reference = 86273;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 85014; DELETE FROM client_documents WHERE cdo_reference = 85014;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86270; DELETE FROM client_documents WHERE cdo_reference = 86270;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 85425; DELETE FROM client_documents WHERE cdo_reference = 85425;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 84260; DELETE FROM client_documents WHERE cdo_reference = 84260;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 84309; DELETE FROM client_documents WHERE cdo_reference = 84309;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 87585; DELETE FROM client_documents WHERE cdo_reference = 87585;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86269; DELETE FROM client_documents WHERE cdo_reference = 86269;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86272; DELETE FROM client_documents WHERE cdo_reference = 86272;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86289; DELETE FROM client_documents WHERE cdo_reference = 86289;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 84248; DELETE FROM client_documents WHERE cdo_reference = 84248;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 85487; DELETE FROM client_documents WHERE cdo_reference = 85487;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 83685; DELETE FROM client_documents WHERE cdo_reference = 83685;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 87586; DELETE FROM client_documents WHERE cdo_reference = 87586;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 87587; DELETE FROM client_documents WHERE cdo_reference = 87587;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86297; DELETE FROM client_documents WHERE cdo_reference = 86297;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 85449; DELETE FROM client_documents WHERE cdo_reference = 85449;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86238; DELETE FROM client_documents WHERE cdo_reference = 86238;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86276; DELETE FROM client_documents WHERE cdo_reference = 86276;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86279; DELETE FROM client_documents WHERE cdo_reference = 86279;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86290; DELETE FROM client_documents WHERE cdo_reference = 86290;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 86274; DELETE FROM client_documents WHERE cdo_reference = 86274;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 84394; DELETE FROM client_documents WHERE cdo_reference = 84394;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 91730; DELETE FROM client_documents WHERE cdo_reference = 91730;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 95138; DELETE FROM client_documents WHERE cdo_reference = 95138;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 99929; DELETE FROM client_documents WHERE cdo_reference = 99929;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 99908; DELETE FROM client_documents WHERE cdo_reference = 99908;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 99963; DELETE FROM client_documents WHERE cdo_reference = 99963;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 98461; DELETE FROM client_documents WHERE cdo_reference = 98461;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 101552; DELETE FROM client_documents WHERE cdo_reference = 101552;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 102867; DELETE FROM client_documents WHERE cdo_reference = 102867;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 102975; DELETE FROM client_documents WHERE cdo_reference = 102975;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 102869; DELETE FROM client_documents WHERE cdo_reference = 102869;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 102932; DELETE FROM client_documents WHERE cdo_reference = 102932;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 102908; DELETE FROM client_documents WHERE cdo_reference = 102908;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 102973; DELETE FROM client_documents WHERE cdo_reference = 102973;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 111692; DELETE FROM client_documents WHERE cdo_reference = 111692;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 114807; DELETE FROM client_documents WHERE cdo_reference = 114807;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 114829; DELETE FROM client_documents WHERE cdo_reference = 114829;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 111711; DELETE FROM client_documents WHERE cdo_reference = 111711;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 111725; DELETE FROM client_documents WHERE cdo_reference = 111725;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 119583; DELETE FROM client_documents WHERE cdo_reference = 119583;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 119635; DELETE FROM client_documents WHERE cdo_reference = 119635;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 122735; DELETE FROM client_documents WHERE cdo_reference = 122735;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 122709; DELETE FROM client_documents WHERE cdo_reference = 122709;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 121321; DELETE FROM client_documents WHERE cdo_reference = 121321;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 128914; DELETE FROM client_documents WHERE cdo_reference = 128914;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 125776; DELETE FROM client_documents WHERE cdo_reference = 125776;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 125827; DELETE FROM client_documents WHERE cdo_reference = 125827;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 135321; DELETE FROM client_documents WHERE cdo_reference = 135321;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 138170; DELETE FROM client_documents WHERE cdo_reference = 138170;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 138169; DELETE FROM client_documents WHERE cdo_reference = 138169;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142523; DELETE FROM client_documents WHERE cdo_reference = 142523;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 139741; DELETE FROM client_documents WHERE cdo_reference = 139741;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 141970; DELETE FROM client_documents WHERE cdo_reference = 141970;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 141984; DELETE FROM client_documents WHERE cdo_reference = 141984;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 141991; DELETE FROM client_documents WHERE cdo_reference = 141991;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142002; DELETE FROM client_documents WHERE cdo_reference = 142002;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142013; DELETE FROM client_documents WHERE cdo_reference = 142013;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142023; DELETE FROM client_documents WHERE cdo_reference = 142023;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142025; DELETE FROM client_documents WHERE cdo_reference = 142025;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142036; DELETE FROM client_documents WHERE cdo_reference = 142036;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142743; DELETE FROM client_documents WHERE cdo_reference = 142743;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142049; DELETE FROM client_documents WHERE cdo_reference = 142049;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142054; DELETE FROM client_documents WHERE cdo_reference = 142054;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142055; DELETE FROM client_documents WHERE cdo_reference = 142055;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142056; DELETE FROM client_documents WHERE cdo_reference = 142056;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142067; DELETE FROM client_documents WHERE cdo_reference = 142067;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142076; DELETE FROM client_documents WHERE cdo_reference = 142076;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142081; DELETE FROM client_documents WHERE cdo_reference = 142081;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142082; DELETE FROM client_documents WHERE cdo_reference = 142082;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142087; DELETE FROM client_documents WHERE cdo_reference = 142087;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142134; DELETE FROM client_documents WHERE cdo_reference = 142134;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142141; DELETE FROM client_documents WHERE cdo_reference = 142141;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142143; DELETE FROM client_documents WHERE cdo_reference = 142143;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142150; DELETE FROM client_documents WHERE cdo_reference = 142150;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142164; DELETE FROM client_documents WHERE cdo_reference = 142164;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142169; DELETE FROM client_documents WHERE cdo_reference = 142169;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142174; DELETE FROM client_documents WHERE cdo_reference = 142174;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142294; DELETE FROM client_documents WHERE cdo_reference = 142294;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 142399; DELETE FROM client_documents WHERE cdo_reference = 142399;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 141536; DELETE FROM client_documents WHERE cdo_reference = 141536;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 141233; DELETE FROM client_documents WHERE cdo_reference = 141233;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 140031; DELETE FROM client_documents WHERE cdo_reference = 140031;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 140720; DELETE FROM client_documents WHERE cdo_reference = 140720;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 140155; DELETE FROM client_documents WHERE cdo_reference = 140155;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 140897; DELETE FROM client_documents WHERE cdo_reference = 140897;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 141064; DELETE FROM client_documents WHERE cdo_reference = 141064;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 141154; DELETE FROM client_documents WHERE cdo_reference = 141154;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 141188; DELETE FROM client_documents WHERE cdo_reference = 141188;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 149125; DELETE FROM client_documents WHERE cdo_reference = 149125;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 149126; DELETE FROM client_documents WHERE cdo_reference = 149126;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 149127; DELETE FROM client_documents WHERE cdo_reference = 149127;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 148627; DELETE FROM client_documents WHERE cdo_reference = 148627;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 148628; DELETE FROM client_documents WHERE cdo_reference = 148628;
DELETE FROM client_document_associations WHERE cda_cdo_reference = 144753; DELETE FROM client_documents WHERE cdo_reference = 144753;

PROMPT Finished Individual Deletes
PROMPT Starting Bulk Deletes

set pages 50000 feed off verify off trimspool on tab off markup csv on
set linesize 3000

CREATE TABLE SAHA_CLIENT_DOCS_CLEANUP AS
SELECT app_refno 
,      app_sco_code
,      cdo_par_refno 
,      cdo_reference 
,      cdo_document_name 
,      cdo_hrv_cdt_code 
FROM applications
JOIN client_document_associations on cda_object_reference = app_refno
                                 and cda_object_type = 'APP'
JOIN client_documents ON cda_cdo_reference = cdo_reference                                 
WHERE NOT EXISTS (select null from applic_list_entries
                  where ale_app_refno = app_refno
                  and ale_rli_code = 'GEN');
				  
SPOOL saha_client_docs_cleanup_bulk_output.csv

SELECT app_refno "Application"
,      app_sco_code "Application Status"
,      cdo_par_refno "Party Reference"
,      cdo_reference "Document Reference"
,      cdo_document_name "Document Name"
,      cdo_hrv_cdt_code "Document Type"
FROM SAHA_CLIENT_DOCS_CLEANUP;

SPOOL OFF

SET feed ON verify ON

DELETE FROM client_document_associations 
WHERE cda_cdo_reference IN (SELECT cdo_reference
                            FROM SAHA_CLIENT_DOCS_CLEANUP);

DELETE FROM client_documents 
WHERE cdo_reference IN (SELECT cdo_reference
                        FROM SAHA_CLIENT_DOCS_CLEANUP);
										  
DROP TABLE SAHA_CLIENT_DOCS_CLEANUP;								  

PROMPT Finished Bulk Deletes
PROMPT Finished VSTS 46193