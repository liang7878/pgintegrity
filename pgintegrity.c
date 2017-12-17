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


char *global_port = "5432";
char *global_password = NULL;
// char *global_password= "940808";

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

char *connectChar(char *str1, char *str2) {
	char *result = palloc(strlen(str1) + strlen(str2));

	if(result == NULL) {
		return NULL;
	} else {
		strcpy(result, str1);
		strcat(result, str2);
		return result;
	}
}

char *randstr(char *pointer, int n) {
    int i,randnum;
	char str_array[63] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	for (i = 0; i < n; i++) {
		srand(time(NULL));
		usleep(100000);
		randnum = rand()%62;
		*pointer = str_array[randnum];
		pointer++;
	}
	*pointer = '\0';
	return (pointer - n);
}

void genRandomString(char* buff, int length) {
    char metachar[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	int i = 0;
	srand((unsigned) time(NULL)); 
	for (i = 0; i < length; i++){
		buff[i] = metachar[rand() % 62];
	}

	buff[length] = '\0';
}


Datum
pg_integrity(PG_FUNCTION_ARGS)
{
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

	if(TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) && TRIGGER_FIRED_AFTER(trigdata->tg_event)) {
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

		char *dbname;
		dbname = get_database_name(currentRel->rd_node.dbNode);

		/* prepare oid and watermark for insert tuple */
		char *verifyresult = getAttrCon(newTuple, insertTupledesc);
		// elog(INFO, "verifyresult source: %s", verifyresult);

		int watermarkint;
		watermarkint = APHash(verifyresult, strlen(verifyresult));

		char *watermarkchar;
		watermarkchar = itostr(watermarkchar, watermarkint);

		char *oidchar;
		oidchar = itostr(oidchar, t_data_Oid);

		char *rand0 = (char *)malloc(sizeof(char) * 20);
		genRandomString(rand0, 10);

		int con;
		if((con = SPI_connect()) < 0){
			elog(ERROR, "SPI_connect error");
		}

		char *select_password_stat = "select current_setting('pgintegrity.password');";
		int select_password = SPI_execute(select_password_stat, true, 0);
		if(select_password == SPI_OK_SELECT) {
			uint64 i;
			for(i=0; i<SPI_processed; i++) {
				global_password = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
				// elog(INFO, "GET password: %s", global_password);
			}
		} else {
			elog(ERROR, "No password: %s", global_password);
		}


		char *insert1 = connectChar("SELECT dblink_exec('", connectChar(rand0, "', 'insert into t_watermark values ("));

		char *insert2 = ",\'\'";
		char *insert3 = "\'\', \'\'";
		char *insert4 = " \'\', \'\'";
		char *insert5 = "\'\')\')";

		char *insertwater = palloc(strlen(insert1)+strlen(oidchar)+strlen(insert2)+strlen(watermarkchar)+strlen(insert3)+strlen(dbname)+strlen(insert4)+ strlen(relName)+ strlen(insert5));

		if(insertwater == NULL) {
		} else {
			strcpy(insertwater, insert1);
			strcat(insertwater, oidchar);
			strcat(insertwater, insert2);
			strcat(insertwater, watermarkchar);
			strcat(insertwater, insert3);
			strcat(insertwater, dbname);
			strcat(insertwater, insert4);
			strcat(insertwater, relName);
			strcat(insertwater, insert5);
		}

		// elog(INFO, "watermarkchar: %s", watermarkchar);


		char *query_current_user = "SELECT CURRENT_USER";
		char *username = NULL;


		select_current_user_res = SPI_execute(query_current_user, true, 0);
		if(select_current_user_res == SPI_OK_SELECT) {
			uint64 i;
			for(i=0; i<SPI_processed; i++) {
				username = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
			}
		}

		if(username == NULL) {
		}

		char *query_user_privilege;



		/* edit your database info here */
		char *qup1 = "SELECT * FROM dblink('hostaddr=127.0.0.1 port=";
		char *qup1_1_1 = " dbname=pgintegrity user=umsuser password=";
		char *qup1_1 = "', 'SELECT user_name, db_name, table_name FROM t_privilege') as t(user_name text, db_name text, table_name text) WHERE user_name='";
		char *qup2 = "' AND db_name='";
		char *qup3 = "' AND table_name='";
		char *qup4 = "'";

		query_user_privilege = palloc(strlen(qup1) + strlen(global_port) + strlen(qup1_1_1) + strlen(global_password) + strlen(qup1_1) +strlen(qup2)+strlen(qup3)+strlen(qup4)+strlen(username)+strlen(dbname)+strlen(relName));

		if(query_user_privilege == NULL) {
		}else{
			strcpy(query_user_privilege, qup1);
			strcat(query_user_privilege, global_port);
			strcat(query_user_privilege, qup1_1_1);
			strcat(query_user_privilege, global_password);
			strcat(query_user_privilege, qup1_1);
			strcat(query_user_privilege, username);
			strcat(query_user_privilege, qup2);
			strcat(query_user_privilege, dbname);
			strcat(query_user_privilege, qup3);
			strcat(query_user_privilege, relName);
			strcat(query_user_privilege, qup4);
		}

		int query_user_privilege_res;
		query_user_privilege_res = SPI_execute(query_user_privilege, true, 0);
		if(query_user_privilege_res == SPI_OK_SELECT) {
			if(SPI_processed >= 1) {
				int connectres;
				int insertres;
				int beginres;
				int commitres;
				int disconnectres;
				/* edit your database info here */
				char *rand1 = (char *)malloc(sizeof(char) * 20);
				genRandomString(rand1, 10);
				// char *connectchartrans = connectChar("SELECT dblink_connect_u('", connectChar(rand0, connectChar(connectChar("', 'hostaddr=127.0.0.1 port=", connectChar(global_port," dbname=pgintegrity user=postgres password=")), connectChar(global_password, "')"))));
				// elog(INFO, "trans connect: %s", connectchartrans);
				// 
				char *connectchartrans1 = "SELECT dblink_connect_u('";
				char *connectchartrans2 = "', 'hostaddr=127.0.0.1 port=";
				char *connectchartrans3 = " dbname=pgintegrity user=umsuser password=";
				char *connectchartrans4 = "')";
				char *connectchartrans = palloc(strlen(connectchartrans1)+strlen(rand0)+strlen(connectchartrans2)+ strlen(global_port) +strlen(connectchartrans3)+ strlen(global_password) +strlen(connectchartrans4));

				if(connectchartrans == NULL) {
				} else {
					strcpy(connectchartrans, connectchartrans1);
					strcat(connectchartrans, rand0);
					strcat(connectchartrans, connectchartrans2);
					strcat(connectchartrans, global_port);
					strcat(connectchartrans, connectchartrans3);
					strcat(connectchartrans, global_password);
					strcat(connectchartrans, connectchartrans4);
				}

				connectres = SPI_execute(connectchartrans, false, 0);
				beginres = SPI_execute(connectChar("SELECT dblink_exec('", connectChar(rand0, "', 'BEGIN')")), false, 0);
				insertres = SPI_execute(insertwater, false, 0);
				commitres = SPI_execute(connectChar("SELECT dblink_exec('", connectChar(rand0, "', 'COMMIT')")), false, 0);
				disconnectres = SPI_execute(connectChar("SELECT dblink_disconnect('", connectChar(rand0, "');")), false, 0);
			} else {
			}
		}

		SPI_finish();
	}

	if(TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) && TRIGGER_FIRED_AFTER(trigdata->tg_event)) {
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

		char *dbname;
		dbname = get_database_name(currentRel->rd_node.dbNode);

		/* prepare oid and watermark for insert tuple */
		char *verifyresult = getAttrCon(newTuple, insertTupledesc);

		int watermarkint;
		watermarkint = APHash(verifyresult, strlen(verifyresult));
		char *watermarkchar;
		watermarkchar = itostr(watermarkchar, watermarkint);

		char *oidchar;
		oidchar = itostr(oidchar, t_data_Oid);

		char *rand1 = (char *)malloc(sizeof(char) * 20);
		genRandomString(rand1, 10);



		int con;
		if((con = SPI_connect()) < 0)
			elog(ERROR, "select spi error");


		char *select_password_stat = "select current_setting('pgintegrity.password');";
		int select_password = SPI_execute(select_password_stat, true, 0);
		if(select_password == SPI_OK_SELECT) {
			uint64 i;
			for(i=0; i<SPI_processed; i++) {
				global_password = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
				// elog(INFO, "GET password: %s", global_password);
			}
		} else {
			elog(ERROR, "No password: %s", global_password);
		}

		char *insert1 = connectChar("SELECT dblink_exec('", connectChar(rand1, "', 'UPDATE t_watermark SET watermark = ''"));
		char *insert2 = "\'\' WHERE oid=";
		char *insert3 = "\')";

		char *insertwater = palloc(strlen(insert1)+strlen(oidchar)+strlen(insert2)+strlen(watermarkchar)+strlen(insert3));

		if(insertwater == NULL) {
		} else {
			strcpy(insertwater, insert1);
			strcat(insertwater, watermarkchar);
			strcat(insertwater, insert2);
			strcat(insertwater, oidchar);
			strcat(insertwater, insert3);
		}


		char *query_current_user = "SELECT CURRENT_USER";
		char *username = NULL;


		select_current_user_res = SPI_execute(query_current_user, true, 0);
		if(select_current_user_res == SPI_OK_SELECT) {
			uint64 i;
			for(i=0; i<SPI_processed; i++) {
				username = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
			}
		}

		if(username == NULL) {
			elog(ERROR, "Get Username Error!");
		}

		char *query_user_privilege;

		/* edit your database info here */
		char *qup1 = "SELECT * FROM dblink('hostaddr=127.0.0.1 port=";
		char *qup1_1 = " dbname=pgintegrity user=umsuser password=";
		char *qup1_1_1 = "', 'SELECT user_name, db_name, table_name FROM t_privilege') as t(user_name text, db_name text, table_name text) WHERE user_name='";
		char *qup2 = "' AND db_name='";
		char *qup3 = "' AND table_name='";
		char *qup4 = "'";

		query_user_privilege = palloc(strlen(qup1) + strlen(global_port) + strlen(qup1_1) + strlen(global_password) +strlen(qup2)+strlen(qup3)+strlen(qup4)+strlen(username)+strlen(dbname)+strlen(relName));

		if(query_user_privilege == NULL) {
		}else{
			strcpy(query_user_privilege, qup1);
			strcat(query_user_privilege, global_port);
			strcat(query_user_privilege, qup1_1);
			strcat(query_user_privilege, global_password);
			strcat(query_user_privilege, qup1_1_1);
			strcat(query_user_privilege, username);
			strcat(query_user_privilege, qup2);
			strcat(query_user_privilege, dbname);
			strcat(query_user_privilege, qup3);
			strcat(query_user_privilege, relName);
			strcat(query_user_privilege, qup4);
		}

		int query_user_privilege_res;
		query_user_privilege_res = SPI_execute(query_user_privilege, true, 0);
		if(query_user_privilege_res == SPI_OK_SELECT) {
			if(SPI_processed >= 1) {
				int connectres;
				int insertres;
				int beginres;
				int commitres;
				int disconnectres;

				char *updatetrans1 = "SELECT dblink_connect_u('";
				char *updatetrans2 = "', 'hostaddr=127.0.0.1 port=";
				char *updatetrans3 = " dbname=pgintegrity user=umsuser password=";
				char *updatetrans4 = "')";

				char *updatetrans = palloc(strlen(updatetrans1)+ strlen(rand1) +strlen(updatetrans2)+ strlen(global_port) +strlen(updatetrans3) + strlen(global_password) + strlen(updatetrans4));

				if(updatetrans == NULL) {
				} else {
					strcpy(updatetrans, updatetrans1);
					strcat(updatetrans, rand1);
					strcat(updatetrans, updatetrans2);
					strcat(updatetrans, global_port);
					strcat(updatetrans, updatetrans3);
					strcat(updatetrans, global_password);
					strcat(updatetrans, updatetrans4);
				}


				/* edit your database info here */
				connectres = SPI_execute(updatetrans, false, 0);
				beginres = SPI_execute(connectChar("SELECT dblink_exec('", connectChar(rand1, "', 'BEGIN')")), false, 0);
				insertres = SPI_execute(insertwater, false, 0);
				commitres = SPI_execute(connectChar("SELECT dblink_exec('", connectChar(rand1, "', 'COMMIT')")), false, 0);
				disconnectres = SPI_execute(connectChar("SELECT dblink_disconnect('", connectChar(rand1, "');")), false, 0);
			} else {
			}
		}

		SPI_finish();
	}

	if(TRIGGER_FIRED_BY_DELETE(trigdata->tg_event) && TRIGGER_FIRED_AFTER(trigdata->tg_event)) {
		oldTuple = trigdata->tg_trigtuple;
		currentRel = trigdata->tg_relation;
		relName = RelationGetRelationName(currentRel);
		char *dbname;
		dbname = get_database_name(currentRel->rd_node.dbNode);
		Oid t_data_Oid = HeapTupleGetOid(oldTuple);
		char *oidchar;
		oidchar = itostr(oidchar, t_data_Oid);		

		int con;
		if((con = SPI_connect()) < 0)
			elog(ERROR, "select spi error");

		char *select_password_stat = "select current_setting('pgintegrity.password');";
		int select_password = SPI_execute(select_password_stat, true, 0);
		if(select_password == SPI_OK_SELECT) {
			uint64 i;
			for(i=0; i<SPI_processed; i++) {
				global_password = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
				// elog(INFO, "GET password: %s", global_password);
			}
		} else {
			elog(ERROR, "No password: %s", global_password);
		}

		char *query_current_user = "SELECT CURRENT_USER";
		char *username = NULL;

		char *rand2 = (char *)malloc(sizeof(char) * 20);
		genRandomString(rand2, 10);

		select_current_user_res = SPI_execute(query_current_user, true, 0);
		if(select_current_user_res == SPI_OK_SELECT) {
			uint64 i;
			for(i=0; i<SPI_processed; i++) {
				username = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
			}
		}

		if(username == NULL) {
			elog(ERROR, "Get Username Error!");
		}

		char *query_user_privilege;

		/* edit your database info here */
		char *qup1 = "SELECT * FROM dblink('hostaddr=127.0.0.1 port=";
		char *qup1_1 = " dbname=pgintegrity user=umsuser password=";
		char *qup1_1_1 = "', 'SELECT user_name, db_name, table_name FROM t_privilege') as t(user_name text, db_name text, table_name text) WHERE user_name='";
		char *qup2 = "' AND db_name='";
		char *qup3 = "' AND table_name='";
		char *qup4 = "'";

		query_user_privilege = palloc(strlen(qup1) + strlen(global_port) + strlen(qup1_1) + strlen(global_password) +strlen(qup2)+strlen(qup3)+strlen(qup4)+strlen(username)+strlen(dbname)+strlen(relName));

		if(query_user_privilege == NULL) {
		}else{
			strcpy(query_user_privilege, qup1);
			strcat(query_user_privilege, global_port);
			strcat(query_user_privilege, qup1_1);
			strcat(query_user_privilege, global_password);
			strcat(query_user_privilege, qup1_1_1);
			strcat(query_user_privilege, username);
			strcat(query_user_privilege, qup2);
			strcat(query_user_privilege, dbname);
			strcat(query_user_privilege, qup3);
			strcat(query_user_privilege, relName);
			strcat(query_user_privilege, qup4);
		}

		int query_user_privilege_res;
		query_user_privilege_res = SPI_execute(query_user_privilege, true, 0);

		if(query_user_privilege_res == SPI_OK_SELECT) {
			if(SPI_processed >= 1) {
				int connectres;
				int insertres;
				int beginres;
				int commitres;
				int disconnectres;

				char *updatetrans1 = "SELECT dblink_connect_u('";
				char *updatetrans2 = "', 'hostaddr=127.0.0.1 port=";
				char *updatetrans3 = " dbname=pgintegrity user=umsuser password=";
				char *updatetrans4 = "')";

				char *updatetrans = palloc(strlen(updatetrans1)+ strlen(rand2) +strlen(updatetrans2)+ strlen(global_port) +strlen(updatetrans3) + strlen(global_password) + strlen(updatetrans4));

				if(updatetrans == NULL) {
				} else {
					strcpy(updatetrans, updatetrans1);
					strcat(updatetrans, rand2);
					strcat(updatetrans, updatetrans2);
					strcat(updatetrans, global_port);
					strcat(updatetrans, updatetrans3);
					strcat(updatetrans, global_password);
					strcat(updatetrans, updatetrans4);
				}




				char *deletewater1 = "select dblink_exec('";
				char *deletewater1_1 = "', 'delete from t_watermark where oid = ";
				char *deletewater2 = "')";
				char *deletewater = palloc(strlen(deletewater1)+strlen(deletewater2)+strlen(oidchar)+strlen(rand2) + strlen(deletewater1_1));

				if(deletewater == NULL) {
				} else {
					strcpy(deletewater, deletewater1);
					strcat(deletewater, rand2);
					strcat(deletewater, deletewater1_1);
					strcat(deletewater, oidchar);
					strcat(deletewater, deletewater2);
				}

				/* edit your database info here */
				connectres = SPI_execute(updatetrans, false, 0);
//				connectres = SPI_execute("SELECT dblink_connect_u('conection', 'hostaddr=127.0.0.1 port=5432 dbname=pgintegrity user=postgres password=940808')", false, 0);
				beginres = SPI_execute(connectChar("SELECT dblink_exec('", connectChar(rand2, "', 'BEGIN')")), false, 0);
				insertres = SPI_execute(deletewater, false, 0);
				commitres = SPI_execute(connectChar("SELECT dblink_exec('", connectChar(rand2, "', 'COMMIT')")), false, 0);
				disconnectres = SPI_execute(connectChar("SELECT dblink_disconnect('", connectChar(rand2, "');")), false, 0);
			} else {
			}
		}

		SPI_finish();
		//elog(ERROR, "break;");
	}

    /* make sure it's called as a trigger at all */
    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "trigf: not called by trigger manager");

    /* tuple to return to executor */
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        rettuple = trigdata->tg_newtuple;
    else
        rettuple = trigdata->tg_trigtuple;


    tupdesc = trigdata->tg_relation->rd_att;


    SPI_finish();

    return PointerGetDatum(rettuple);
}
