/**
 *	Berkeley db Read Uncommitted isolation example
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
			      DB_THREAD |
			      DB_INIT_TXN;
	const char* home = "envHome";
	
	u_int32_t db_flags = DB_CREATE | DB_AUTO_COMMIT | DB_READ_UNCOMMITTED;
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
				DB_BTREE,	//Database type
				db_flags,	//Open flags
				0		//file mode
			);
	}catch(DbException &e){
		std::cerr<<"Error when opening database and Environment:"
			<<fileName<<","<<home<<std::endl;
		std::cerr<<e.what()<<std::endl;
	}

	//put data normally
	char *key1 = "luffy";
	char *data1 = "op";
	char *key2= "usopp";
	char *data2 = "brave";

	Dbt pkey1(key1,strlen(key1)+1);
	Dbt pdata1(data1,strlen(data1)+1);
	Dbt pkey2(key2,strlen(key2)+1);
	Dbt pdata2(data2,strlen(data2)+1);	

	dbp->put(NULL,&pkey1,&pdata1,0);
	dbp->put(NULL,&pkey2,&pdata2,0);

	//using txn cursor to read and another cursor to modify before commit
	try{
		DbTxn *txn1 = NULL;
		myEnv.txn_begin(NULL,&txn1,0);
		Dbc *cursorp = NULL;
		dbp->cursor(txn1,&cursorp,0);
		Dbt tempData2;
		tempData2.set_flags(DB_DBT_MALLOC);           //ask BDB to malloc a space to put the data or user Usermemory
		cursorp->put(&pkey1,&pdata2,DB_KEYFIRST);
		cursorp->put(&pkey2,&pdata1,DB_KEYFIRST);   
		
		//get the dirty data
		dbp->get(NULL,&pkey1,&tempData2,DB_READ_UNCOMMITTED);  //first param is NULL for "DB_AUTO_COMMITTED" flag 
								       //we can read dirty data here and no block.no matter the txn1 within 
								       //"DB_READ_UNCOMMITTED" or not	
		//cout
		std::cout<<(char*)pkey1.get_data()<<" : "<<(char*)tempData2.get_data()<<std::endl;
		//close cursor
		cursorp->close();
		//commit the txn
		txn1->commit(0);
		//if txn1 abort , the changed data will never store in database.
		//txn1->abort();
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
 */
