#include "postgres.h"
#include "funcapi.h"

#include <stdlib.h>

#include "access/htup_details.h"
#include "executor/spi.h"       /* this is what you need to work with SPI */
#include "commands/trigger.h"   /* ... triggers ... */
#include "utils/rel.h"          /* ... and relations */
//#include "access/Htup_details.h"
// #include "sha.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_integrity);

/* transfer int to char*  */
char* itostr(char *str, int num) //将i转化位字符串存入str
{
	str = (char *) palloc(64 * sizeof(char));
    sprintf(str, "%d", num);
    return str;
}


unsigned int APHash(char* str, unsigned int len)
{
   unsigned int hash = 0xAAAAAAAA;
   unsigned int i    = 0;
   for(i = 0; i < len; str++, i++)
   {
      hash ^= ((i & 1) == 0) ? (  (hash <<  7) ^ (*str) * (hash >> 3)) :
                               (~((hash << 11) + (*str) ^ (hash >> 5)));
   }
   return hash;
}
/* End Of AP Hash Function */ 


static List *
query_to_oid_list(const char *query)
{
	uint64		i;
	List	   *list = NIL;

	SPI_execute(query, true, 0);

	for (i = 0; i < SPI_processed; i++)
	{
		Datum		oid;
		bool		isnull;

		oid = SPI_getbinval(SPI_tuptable->vals[i],
							SPI_tuptable->tupdesc,
							1,
							&isnull);
		if (!isnull)
			list = lappend_oid(list, DatumGetObjectId(oid));
	}

	return list;
}


char *getAttrCon(HeapTuple tuple, TupleDesc tupdesc) {
	int attrNum;		//column num of a tuple
	attrNum = tupdesc->natts;
	int natts;
	natts = attrNum;
	char *attrtext[attrNum];

	int i;

	for(i=0; i<natts; i++) {
		Datum val;
		bool isnull;
		Oid typoid, foutoid;
		bool typisvarlena;
		int fnumber = i+1;
		char *resultchar;

		val = heap_getattr(tuple, fnumber, tupdesc, &isnull);

		if(fnumber > 0) {
			typoid = tupdesc->attrs[fnumber-1]->atttypid;
			getTypeOutputInfo(typoid, &foutoid, &typisvarlena);
			resultchar = OidOutputFunctionCall(foutoid, val);

			attrtext[i] = resultchar;
		}
	}

	int totallength = 0;

	for(i=0; i<natts; i++) {
		totallength = totallength + strlen(attrtext[i]);
	}

	char *result;
	result = palloc(totallength);
	if(result == NULL) {

	}else {
		strcpy(result, attrtext[0]);
		for(i=1; i<natts; i++) {
			strcat(result, attrtext[i]);
		}
	}

	return result;
}


Datum
pg_integrity(PG_FUNCTION_ARGS)
{
	elog(INFO, "enter the function");
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    TupleDesc   tupdesc;
    HeapTuple   rettuple;
    char       *when;
    bool        checknull = false;
    bool        isnull;
    int         ret, i;

	
	bool isUpdate = false;
	bool isInsert = false;
	HeapTuple oldTuple = NULL;
	HeapTuple newTuple = NULL;
	TupleDesc insertTupledesc = NULL;
	Relation currentRel = NULL;
	int ncolumns;
	Datum *insertvalue;
	bool *insertisnull;
	char *relName = NULL;
	int selectres;
	List *list = NIL;	//used to store select result
	int select_current_user_res;
//	List *oidlist = NIL;

	elog(INFO, "before fire by insert");
	if(TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) && TRIGGER_FIRED_AFTER(trigdata->tg_event)) {
		elog(INFO, "enter fire insert 1");
		
		isUpdate = true;
		newTuple = trigdata->tg_trigtuple;
		currentRel = trigdata->tg_relation;
		relName = RelationGetRelationName(currentRel);
		insertTupledesc = RelationNameGetTupleDesc(relName);
		ncolumns = insertTupledesc->natts;
		insertvalue = (Datum *) palloc(ncolumns * sizeof(Datum));
		insertisnull = (bool *) palloc(ncolumns * sizeof(bool));
		heap_deform_tuple(newTuple, insertTupledesc, insertvalue, insertisnull);
		Oid t_data_Oid = HeapTupleGetOid(newTuple);	//oid of insert tuple
		// elog(INFO, "Oiddidididi: %i", t_data_Oid);
		
		char *dbname;
		dbname = get_database_name(currentRel->rd_node.dbNode);

		elog(INFO, "current relation name: %s", relName);
		elog(INFO, "current database name: %s", dbname);
		/* prepare oid and watermark for insert tuple */
		char *verifyresult = getAttrCon(newTuple, insertTupledesc);
		elog(INFO, "CON result: %s", verifyresult);

		int watermarkint;
		watermarkint = APHash(verifyresult, strlen(verifyresult));

		char *watermarkchar;
		watermarkchar = itostr(watermarkchar, watermarkint);

		char *oidchar;
		oidchar = itostr(oidchar, t_data_Oid);


		char *insert1 = "INSERT INTO t_watermark VALUES (";
		char *insert2 = ",\'";
		char *insert3 = "\')";

		char *insertwater = palloc(strlen(insert1)+strlen(oidchar)+strlen(insert2)+strlen(watermarkchar)+strlen(insert3));

		if(insertwater == NULL) {
		} else {
			strcpy(insertwater, insert1);
			strcat(insertwater, oidchar);
			strcat(insertwater, insert2);
			strcat(insertwater, watermarkchar);
			strcat(insertwater, insert3);
			elog(INFO, "insert watermark state: %s", insertwater);
		}

		int con;
		if((con = SPI_connect()) < 0)
			elog(ERROR, "select spi error");

		char *query_current_user = "SELECT CURRENT_USER";
		char *username = NULL;


		select_current_user_res = SPI_execute(query_current_user, true, 0);
		if(select_current_user_res == SPI_OK_SELECT) {
			uint64 i;
			for(i=0; i<SPI_processed; i++) {
				username = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
				elog(INFO, "current user: %s", username);
			}
		}

		if(username == NULL) {
			elog(ERROR, "Get Username Error!");
		}

		char *query_user_privilege;



		/* edit your database info here */
		char *qup1 = "SELECT * FROM dblink('hostaddr=127.0.0.1 port=5432 dbname=pgintegrity user=postgres password=940808', 'SELECT user_name, db_name, table_name FROM t_privilege') as t(user_name text, db_name text, table_name text) WHERE user_name='";
		char *qup2 = "' AND db_name='";
		char *qup3 = "' AND table_name='";
		char *qup4 = "'";

		query_user_privilege = palloc(strlen(qup1)+strlen(qup2)+strlen(qup3)+strlen(qup4)+strlen(username)+strlen(dbname)+strlen(relName));

		if(query_user_privilege == NULL) {
		}else{
			strcpy(query_user_privilege, qup1);
			strcat(query_user_privilege, username);
			strcat(query_user_privilege, qup2);
			strcat(query_user_privilege, dbname);
			strcat(query_user_privilege, qup3);
			strcat(query_user_privilege, relName);
			strcat(query_user_privilege, qup4);
		}
		
		elog(INFO, "query_user_privilege: %s", query_user_privilege);
//		char *query = "SELECT * FROM t_city where oid = 221515";

//		selectres = SPI_execute(query, true, 0);
		
//		if(selectres == SPI_OK_SELECT) {
//			uint64 i;
//			for(i=0; i< SPI_processed; i++) {
//				bool isnull;
//				
//				if(!isnull){
//					char *test;
//					test = getAttrCon(SPI_tuptable->vals[i], SPI_tuptable->tupdesc);
//					int testwater;
//					testwater = APHash(test, strlen(test));
//					char *testwaterchar;
//					testwaterchar = itostr(testwaterchar, testwater);
//				}
//			}
//		}
		
		int insertres;

		insertres = SPI_execute(insertwater, false, 0);

		SPI_finish();
	}

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) && TRIGGER_FIRED_AFTER(trigdata->tg_event)) {
		isUpdate = true;
		newTuple = trigdata->tg_newtuple;
		currentRel = trigdata->tg_relation;
		relName = RelationGetRelationName(currentRel);
		insertTupledesc = RelationNameGetTupleDesc(relName);
		ncolumns = insertTupledesc->natts;
		insertvalue = (Datum *) palloc(ncolumns * sizeof(Datum));
		insertisnull = (bool *) palloc(ncolumns * sizeof(bool));
		heap_deform_tuple(newTuple, insertTupledesc, insertvalue, insertisnull);
		Oid t_data_Oid = HeapTupleGetOid(newTuple);	//oid of insert tuple

		/* prepare oid and watermark for insert tuple */
		char *verifyresult = getAttrCon(newTuple, insertTupledesc);

		int watermarkint;
		watermarkint = APHash(verifyresult, strlen(verifyresult));
		char *watermarkchar;
		watermarkchar = itostr(watermarkchar, watermarkint);

		char *oidchar;
		oidchar = itostr(oidchar, t_data_Oid);

		char *insert1 = "UPDATE t_watermark SET watermark = \'";
		char *insert2 = "\' WHERE oid = ";

		char *insertwater = palloc(strlen(insert1)+strlen(oidchar)+strlen(insert2)+strlen(watermarkchar));

		if(insertwater == NULL) {
		} else {
			strcpy(insertwater, insert1);
			strcat(insertwater, watermarkchar);
			strcat(insertwater, insert2);
			strcat(insertwater, oidchar);
		}

		int con;
		if((con = SPI_connect()) < 0)
			elog(ERROR, "select spi error");

		char *query = "SELECT * FROM t_city where oid = 221515";

		selectres = SPI_execute(query, true, 0);

		if(selectres == SPI_OK_SELECT) {
			uint64 i;
			for(i=0; i< SPI_processed; i++) {
				bool isnull;

				if(!isnull){
					char *test;
					test = getAttrCon(SPI_tuptable->vals[i], SPI_tuptable->tupdesc);
					int testwater;
					testwater = APHash(test, strlen(test));
					char *testwaterchar;
					testwaterchar = itostr(testwaterchar, testwater);
				}
			}
		}

		int insertres;

		insertres = SPI_execute(insertwater, false, 0);

		SPI_finish();
	}


    /* make sure it's called as a trigger at all */
    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "trigf: not called by trigger manager");

    /* tuple to return to executor */
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        rettuple = trigdata->tg_newtuple;
    else
        rettuple = trigdata->tg_trigtuple;

    /* check for null values */
    if (!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)
        && TRIGGER_FIRED_BEFORE(trigdata->tg_event))
        checknull = true;

    if (TRIGGER_FIRED_BEFORE(trigdata->tg_event))
        when = "before";
    else
        when = "after ";

    tupdesc = trigdata->tg_relation->rd_att;

///    /* connect to SPI manager */
//    if ((ret = SPI_connect()) < 0)
//        elog(ERROR, "trigf (fired %s): SPI_connect returned %d", when, ret);

    /* get number of rows in table */
//    ret = SPI_exec("SELECT count(*) FROM ttest", 0);

//    if (ret < 0)
  //      elog(ERROR, "trigf (fired %s): SPI_exec returned %d", when, ret);

    /* count(*) returns int8, so be careful to convert */
//    i = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
//                                    SPI_tuptable->tupdesc,
//                                    1,
//                                    &isnull));


    SPI_finish();

//    if (checknull)
//    {
//        SPI_getbinval(rettuple, tupdesc, 1, &isnull);
//        if (isnull)
//            rettuple = NULL;
//    }

    return PointerGetDatum(rettuple);
}