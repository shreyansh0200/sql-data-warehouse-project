--create database warehouse

use master;
go

create database Datawarehouse;
go
use Datawarehouse;
go
create schema bronze;go

create schema silver;go
create schema gold;
