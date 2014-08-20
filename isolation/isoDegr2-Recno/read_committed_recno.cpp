/**
 *	Berkeley db Read Committed isolation example
 *  
 *	Copyright 2014 zhangxiaojian <xiaojian_whu@163.com> <www.zhangxiaojian.name>
 */

#include"db_cxx.h"
#include<iostream>
#include<cstring>

int main()
{
	u_int32_t env_flags = DB_CREATE |
			      DB_INIT_LOCK |
			      DB_INIT_LOG |
			      DB_INIT_MPOOL |
			      DB_INIT_TXN;
	const char* home = "envHome";
	
	u_int32_t db_flags = DB_CREATE | DB_AUTO_COMMIT;
	const char* fileName = "envtest.db";
	Db* dbp = NULL;

	DbEnv myEnv(0);
	
	try{
		myEnv.open(home,env_flags,0);
		
		dbp = new Db(&myEnv,0);
		dbp->open(	
				NULL,		//Txn pointer
				fileName,	//File name
				NULL,		//Logic db name
				DB_RECNO,	//Database type
				db_flags,	//Open flags
				0		//file mode
			);
	}catch(DbException &e){
		std::cerr<<"Error when opening database and Environment:"
			<<fileName<<","<<home<<std::endl;
		std::cerr<<e.what()<<std::endl;
	}

	//put data normally
	int key1 = 0;
	char *data1 = "op";
	int key2= 0;
	char *data2 = "brave";

	Dbt pkey1;
	pkey1.set_data(&key1);
	pkey1.set_ulen(sizeof(int));
	pkey1.set_flags(DB_DBT_USERMEM);
	Dbt pdata1(data1,strlen(data1)+1);
	Dbt pkey2(&key2,sizeof(int));
	Dbt pdata2(data2,strlen(data2)+1);	

	dbp->put(NULL,&pkey1,&pdata1,DB_APPEND);
	dbp->put(NULL,&pkey2,&pdata2,DB_APPEND);

	//using txn cursor to read and another cursor to modify before commit
	try{
		DbTxn *txn1 = NULL;
		myEnv.txn_begin(NULL,&txn1,DB_READ_COMMITTED);
		Dbc *cursorp = NULL;
		dbp->cursor(txn1,&cursorp,0);
		Dbt tempData1,tempKey2,tempData2;
		tempData2.set_flags(DB_DBT_MALLOC);
		cursorp->get(&pkey1,&tempData1,DB_SET);
		cursorp->get(&tempKey2,&tempData2,DB_NEXT);
		//cout just to see if it is right
		std::cout<<*((int*)pkey1.get_data())<<" : "<<(char*)tempData1.get_data()<<std::endl
		     	<<*((int*)tempKey2.get_data())<<" : "<<(char*)tempData2.get_data()<<std::endl;
		//txn2 to modify 
		DbTxn *txn2 = NULL;
		myEnv.txn_begin(NULL,&txn2,0);
		//modify the first record with the next record's data
		dbp->put(txn2,&pkey1,&tempData2,0);  //after try, this put method will wait forever becase recno method is not record-level lock
		//close cursor
		cursorp->close();
		//commit the txn
		txn1->commit(0);
		txn2->commit(0);
	}catch(DbException &e){
		std::cerr<<"Error when use a txn"<<std::endl;
	}

	try{
		dbp->close(0);		//dbp should close before environment
		myEnv.close(0);
	}catch(DbException &e){
		std::cerr<<"Error when closing database and environment:"
			<<fileName<<","<<home<<std::endl;
		std::cerr<<e.what()<<std::endl;
	}
	
	return 0;
}



/*
 * 1.flag DB_INIT_MPOOL is necessary
 * 2.flag DB_AUTO_COMMIT is necessary to make a database support txn
 * 3.Environment's director must exist , application won't create for you
 * 4.line 45 Dbt use USERMEMORY is necessary, for the int type key will be the record id , and the Dbt data area will point to it;
 * 5.After excute , dead lock is accur.......prove recno access is not record-level lock
 */
