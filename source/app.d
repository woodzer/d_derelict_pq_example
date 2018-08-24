import std.array;
import std.conv;
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

string getResultErrorMessage(PGresult *result) {
    return(fromStringz(PQresultErrorMessage(result)).idup);
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

/**
 * This function deletes the contents of the accounts_test table.
 */
bool emptyTable(PGconn* connection) {
    auto   result = PQexec(connection, toStringz("delete from accounts_test"));
    auto   status = PQresultStatus(result);

    scope(exit) PQclear(result);
    if(status != PGRES_COMMAND_OK) {
        writeln("Delete table records failed.  Reason: ", fromStringz(PQresultErrorMessage(result)).idup);
    } else {
        writeln("Table records successfully deleted.");
    }

    return(status == PGRES_COMMAND_OK);
}

/**
 * This function empties the accounts_test table and then drops it.
 */
bool dropTable(PGconn *connection) {
    bool outcome = emptyTable(connection);

    if(outcome) {
        auto   result = PQexec(connection, toStringz("drop table accounts_test"));
        auto   status = PQresultStatus(result);

        scope(exit) PQclear(result);
        if(status != PGRES_COMMAND_OK) {
            writeln("Drop table failed.  Reason: ", fromStringz(PQresultErrorMessage(result)).idup);
        } else {
            writeln("Table successfully dropped.");
            outcome = true;
        }        
    }

    return(outcome);
}

/**
 * This function inserts 1000 rows into the accounts_test table.
 */
void populateAccountsTable(PGconn *connection) {
	string         sql    = "insert into accounts_test(user_name, balance, created_at, updated_at) " ~
	                        "values($1::varchar, $2::real, $3::timestamp, $3::timestamp) returning id";
	auto           random = Random(cast(int)(Clock.currTime().toUnixTime()));
    PGresult       *result;
    ExecStatusType status;
    bool           successful = true;

    writeln("Starting the create records transaction.");
    result = PQexec(connection, toStringz("begin transaction"));
    status = PQresultStatus(result);
    if(status != PGRES_COMMAND_OK) {
        throw(new Exception("Failed to start transaction, return status was " ~ getResultStatusName(result) ~ "."));
    }
    PQclear(result);
    writeln("Create records transaction successfully started.");

    try {
        writeln("About to enter loop.");
	    for(auto i = 0; i < 1000; i++) {
            float     balanceValue     = uniform(0.0, 1000.0, random);
	    	auto      email            = format("email.%05d@nowhere.com", i),
	    	          now              = formatDateTime(cast(DateTime)Clock.currTime()),
	    	          balance          = format("%g", balanceValue);
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

            writeln(format("Creating Account: email= %s, balance=%.02f (%s)", email, balanceValue, balance));
            result = PQexecParams(connection,
                                  toStringz(sql),
                                  parameters.length,
                                  cast(const(uint)*)(null),
                                  cast(const ubyte**)(parameters.ptr),
                                  cast(const int*)(parameterLengths.ptr),
                                  cast(const int*)(parameterFormats.ptr),
                                  1);
            scope(exit) PQclear(result);

            status = PQresultStatus(result);
            if(status != PGRES_TUPLES_OK) {
                throw(new Exception(format("Failed to create database row, return status was %s.", getResultStatusName(result))));
            }
	    }
	} catch(Exception exception) {
        writeln("Exception caught rolling back transaction.");
		auto outcome = PQexec(connection, toStringz("rollback transsaction"));
        successful = false;
		PQclear(outcome);
        throw(exception);
	}

    if(successful) {
        writeln("Committing the create records transaction.");
        result = PQexec(connection, toStringz("commit"));
        status = PQresultStatus(result);
        if(status != PGRES_COMMAND_OK) {
            throw(new Exception("Failed to commit transaction, return status was " ~ getResultStatusName(result) ~ "."));
        }
        scope(exit) PQclear(result);
        writeln("Create records transaction successfully committed.");
    }
}

/**
 * This function list records in the accounts_test table with a balance greater
 * than 500.0.
 */
void listTableContents(PGconn* connection) {
    string         sql    = "select id, user_name, balance, created_at, updated_at " ~
                            "from accounts_test where balance > $1::float";
    PGresult       *result;
    ExecStatusType status;
    string         balanceText = "500.0";
    ubyte*[1]      parameters;
    int[1]         parameterLengths,
                   parameterFormats = [0];

    parameters[0]       = cast(ubyte*)toStringz(balanceText);
    parameterLengths[0] = cast(int)(balanceText.length + 1);
    result = PQexecParams(connection,
                          toStringz(sql),
                          parameters.length,
                          cast(const(uint)*)(null),
                          cast(const ubyte**)(parameters.ptr),
                          cast(const int*)(parameterLengths.ptr),
                          cast(const int*)(parameterFormats.ptr),
                          0);
    scope(exit) PQclear(result);

    status = PQresultStatus(result);
    writeln(format("RESULT: %s\n", getResultStatusName(result)));
    if(PQresultStatus(result) != PGRES_TUPLES_OK) {
        throw(new Exception("Select failed. Reason: " ~ getResultErrorMessage(result)));
    }

    auto     rowCount    = PQntuples(result),
             columnCount = PQnfields(result);
    string[] columnNames;

    for(auto i = 0; i < columnCount; i++) {
        string name = fromStringz(PQfname(result, i)).idup;

        writeln(format("%d: %s", i, name));
        columnNames ~= name;
    }
    writeln("Column Names: ", columnNames.join(", "));

    for(auto row = 0; row < rowCount; row++) {
        int      id;
        string   email,
                 value;
        float    balance;
        string   createdAt,
                 updatedAt;

        value = fromStringz(cast(char*)PQgetvalue(result, row, 0)).idup;
        id    = to!int(value);

        value = fromStringz(cast(char*)PQgetvalue(result, row, 1)).idup;
        email = value;

        value   = fromStringz(cast(char*)PQgetvalue(result, row, 2)).idup;
        balance = to!float(value);

        value     = fromStringz(cast(char*)PQgetvalue(result, row, 3)).idup;
        createdAt = value;

        value     = fromStringz(cast(char*)PQgetvalue(result, row, 3)).idup;
        updatedAt = value;

        writeln(format("ROW: id=%d, email='%s', balance=%.02f, created_at=%s, updated_at=%s",
                       id, email, balance, createdAt, updatedAt));
    }
    writeln("Listed ", rowCount, " records with a balance greater than ", balanceText, ".");
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
            listTableContents(connection);
            dropTable(connection);
		} else {
			writeln("Creation of the accounts_test failed, no further functionality may be attempted.");
		}

	} catch(Exception exception) {
		writeln("ERROR: ", exception.message);
	}
}
