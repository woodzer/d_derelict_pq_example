import std.array;
import std.bitmanip;
import std.datetime;
import std.format;
import std.random;
import std.stdio;
import std.string;
import std.system;
import derelict.pq.pq;

/**
 * Postgres using network byte order, also known as big endian byte order, so
 * numeric values need translation on little endian systems, and that's what
 * this function does.
 */
T endianAdjust(T)(T value) {
	return(endian == Endian.littleEndian ? swapEndian(value) : value);
}

string formatDateTime(DateTime value) {
	return(format("%d-%02d-%02d %02d:%02d:%02d", value.year, value.month, value.day, value.hour, value.minute, value.second));
}

string getResultStatusName(PGresult *result) {
	return(fromStringz(PQresStatus(PQresultStatus(result))).idup);
}

/**
 * This function creates an accounts_test table in the attached database that
 * will be used by subsequent functions.
 */
bool createTable(PGconn *connection) {
	string sql = "create table if not exists accounts_test(id serial primary key, " ~
	                                                      "user_name varchar(100) not null unique, " ~
	                                                      "balance numeric(5,2) not null default 0, " ~
	                                                      "created_at timestamp not null, " ~
	                                                      "updated_at timestamp)";
    auto   result = PQexec(connection, toStringz(sql));
    auto   status = PQresultStatus(result);

    scope(exit) PQclear(result);
    if(status != PGRES_COMMAND_OK) {
    	writeln("Table creation failed. Status: ", getResultStatusName(result), ", Reason: ", fromStringz(PQresultErrorMessage(result)).idup);
    }

    return(status == PGRES_COMMAND_OK);
}

void populateAccountsTable(PGconn *connection) {
	string sql = "insert into accounts_test(user_name, balance, created_at, updated_at) " ~
	             "values($1::varchar, $2::real, $3::timestamp, $3::timestamp) returning id";
	auto   random = Random(1000);

    try {
	    for(auto i = 0; i < 1000; i++) {
	    	auto      email            = format("email.%05d@nowhere.com", i),
	    	          now              = formatDateTime(cast(DateTime)Clock.currTime()),
	    	          balance          = format("%f", uniform(0.0, 1000.0, random));
	    	ubyte*[3] parameters;
	    	uint*     parameterTypes   = cast(uint*)(null);
            int[3]    parameterLengths,
                      parameterFormats = [0, 0, 0];
            
            parameterLengths[0] = cast(int)(email.length + 1);
            parameterLengths[1] = cast(int)(balance.length + 1);
            parameterLengths[2] = cast(int)(now.length + 1);

            parameters[0] = cast(ubyte*)(toStringz(email));
            parameters[1] = cast(ubyte*)(toStringz(balance));
            parameters[2] = cast(ubyte*)(toStringz(now));

            writeln(format("Creating Account: email= %s, balance=%.02f", email, balance));
            auto result = PQexecParams(connection,
            	                       toStringz(sql),
            	                       parameters.length,
            	                       cast(const(uint)*)(null),
		    	                       cast(const ubyte**)(parameters.ptr),
		    	                       cast(const int*)(parameterLengths.ptr),
		    	                       cast(const int*)(parameterFormats.ptr),
            	                       1);
            scope(exit) PQclear(result);
	    }
	} catch(Exception exception) {
		auto outcome = PQexec(connection, toStringz("rollback transsaction"));
		PQclear(outcome);
	}
}

void selectExample(PGconn *connection) {
	string    sql         = "select * from users where id = $1::int4";
	int       userId      = endianAdjust(1),
	          format      = 1;
	ubyte*[1] parameters;
	uint[1]   parameterTypes;
    int[1]    parameterLengths,
              parameterFormats;

	parameters[0]       = cast(ubyte*)(&userId);
	parameterTypes[0]   = 0;
    parameterLengths[0] = userId.sizeof;
    parameterFormats[0] = format;

    auto result = PQexecParams(connection,
    	                       	toStringz(sql),
    	                       	parameters.length,
    	                       	cast(const(uint)*)(parameterTypes.ptr),
    	                       	cast(const ubyte**)(parameters.ptr),
    	                       	cast(const int*)(parameterLengths.ptr),
    	                       	cast(const int*)(parameterFormats.ptr),
    	                       	format);
    scope(exit) PQclear(result);

    if(PQresultStatus(result) == PGRES_TUPLES_OK) {
    	auto     columnCount = PQnfields(result),
    	         rowCount    = PQntuples(result);
    	string[] columnNames;

    	writeln("Query was successful, there are ", rowCount, " rows containing ", columnCount, " columns each in the result.");
    	for(auto i = 0; i < columnCount; i++) {
    		string name = fromStringz(PQfname(result, i)).idup;
    		columnNames ~= name;
    	}
    	writeln("  Column Names: ", columnNames.join(", "));
    } else {
    	throw(new Exception("Select statement failed."));
    }
}

void main() {
	try {
		DerelictPQ.load();

		auto      databaseURL = "postgres://gtt@localhost:5432/gtt_main_test";
		auto      connection  = PQconnectdb(toStringz(databaseURL));

		scope(exit) PQfinish(connection);


        writeln("Application Endianness: ", endian);

		//selectExample(connection);
		if(createTable(connection)) {
			writeln("The accounts_test table was successfully created.");
			populateAccountsTable(connection);
		} else {
			writeln("Creation of the accounts_test failed, no further functionality may be attempted.");
		}

	} catch(Exception exception) {
		writeln("ERROR: ", exception.message);
	}
}
