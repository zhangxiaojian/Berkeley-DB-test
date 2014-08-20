/**
 *	Berkeley db Default isolation example
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
				DB_BTREE,	//Database type
				db_flags,	//Open flags
				0			//file mode
			);
	}catch(DbException &e){
		std::cerr<<"Error when opening database and Environment:"
			<<fileName<<","<<home<<std::endl;
		std::cerr<<e.what()<<std::endl;
	}

	//put data normally
	char *key1 = "luffy";
	char *data1 = "op";
	char *key2 = "usopp";
	char *data2 = "brave";

	Dbt pkey1(key1,strlen(key1)+1);
	Dbt pdata1(data1,strlen(data1)+1);
	Dbt pkey2(key2,strlen(key2)+1);
	Dbt pdata2(data2,strlen(data2)+1);	

	dbp->put(NULL,&pkey1,&pdata1,0);
	dbp->put(NULL,&pkey2,&pdata2,0);

	//using txn cursor to read and another txn to modify before commit
	try{
		DbTxn *txn1 = NULL;
		myEnv.txn_begin(NULL,&txn1,0);
		Dbc *cursorp = NULL;
		dbp->cursor(txn1,&cursorp,0);
		Dbt tempData1,tempKey2,tempData2;
		tempData2.set_flags(DB_DBT_MALLOC);
		cursorp->get(&pkey1,&tempData1,DB_SET);
		cursorp->get(&tempKey2,&tempData2,DB_NEXT);
		//cout just to see if it is right
		std::cout<<(char*)pkey1.get_data()<<" : "<<(char*)tempData1.get_data()<<std::endl
		     	<<(char*)tempKey2.get_data()<<" : "<<(char*)tempData2.get_data()<<std::endl;
		delete tempData2.get_data();
		//txn2 to modify 
		DbTxn *txn2 = NULL;
		myEnv.txn_begin(NULL,&txn2,0);
		//modify the first record with the next record's data
		dbp->put(txn2,&pkey1,&tempData2,0);  //after try, this put method will wait the txn1 commit forever....
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
 * 4.After excute , the selfdead lock is accur for default isolation degree..
 */
