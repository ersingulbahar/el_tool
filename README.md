# el_tool
el with python




KAFKA

    python run.py --statement "read" --topic "test.sqlite-jdbc-ttech" --file_name "test.csv" --environment "kafka_read_test"
    python run.py --statement "write" --topic "test.sqlite-jdbc-ttech" --file_name "test.csv" --environment "kafka_write_test" 


ORACLE
    
    python run.py --statement "db2file" --file_name "test.csv" --environment "dds_prod" --query " select * from ersin.denemeeeee2" --batch "10000" --mode "w" --enable_parallel "True" -pc 2 -tc 2
    python run.py --statement "file2db" --file_name "test.csv" --environment "dds_test" --table_name "stutuncu.denemeeeee" --batch "150000"
    python run.py --statement "db2db" --environment "dds_test" --target_environment "dds_test" --query "select * from stutuncu.denemeeeee2" --table_name "stutuncu.denemeeeee" --mode "w" --batch 20 --enable_parallel "True" -pc 2 -tc 2
    python run.py --statement "execute" --type "oracle" --environment "dds_test"


POSTGRE

    python run.py --statement "db2file" --file_name "test.csv" --environment "postgresql_test" --query "select * from AB_TT164209.TRANSACTIONS_TT164209"
    python run.py --statement "file2db" --file_name "test.csv" --environment "postgresql_test" --table_name "AB_TT164209.TRANSACTIONS_TT164209"
    python run.py --statement "db2db" --environment "postgresql_test" --target_environment "dds_test" --query "select * from AB_TT164209.TRANSACTIONS_TT164209" --table_name "stutuncu.postgreden" --mode "w" --batch 250


MSSQL

     python run.py --statement "db2file" --environment "mssql_dev" --query "select * from ExcentiveDW.dbo.Ayin_Illeri" --file_name "test.csv"
     python run.py --statement "db2db" --environment "mssql_dev" --target_environment "dds_test" --query "select * from ExcentiveDW.dbo.Ayin_Illeri" --table_name "stutuncu.Ayin_Illeri" --mode "w" --batch 250
     

SAPHANA
    
     python run.py --statement "db2file" --environment "saphana_test" --query "SELECT 1 AS id FROM dummy" --file_name "test.csv"
     python run.py --statement "file2db" --file_name "test.csv" --environment "saphana_test" --table_name "dwh.denemeeeee" --batch "150000"
     python run.py --statement "db2db" --environment "dds_prod" --target_environment "saphana_test" --query "select * from ersin.denemeeeee" --table_name "dwh.denemeeeee" --mode "w" --batch 50000 --enable_parallel "True" -pc 4 -tc 4
     python run.py --statement "db2file" --environment "saphana_test" --query "select * from dwh.denemeeeee" --file_name "test.csv" --enable_parallel "True" -pc 4 -tc 4 --unique_column "AAA" --batch "10000"



from __init__ import EL

e = EL(statement = "db2file" ,
        file_name = "test2.csv" ,
        environment = "dds_prod" ,
        query = " select * from ersin.denemeeeee2" ,
        batch = "10000" ,
        mode = "w" )

e.run()
