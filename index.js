const cassandra = require('cassandra-driver');
const async = require('async');
const assert = require('assert');

// Create a YB CQL client.
// DataStax Nodejs 4.0 loadbalancing default is TokenAwarePolicy with child DCAwareRoundRobinPolicy
// Need to provide localDataCenter option below or switch to RoundRobinPolicy
const loadBalancingPolicy = new cassandra.policies.loadBalancing.RoundRobinPolicy ();
const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], policies : { loadBalancing : loadBalancingPolicy }});

async.series([
  function connect(next) {
    client.connect(next);
  },
  function createKeyspace(next) {
    console.log('Creating keyspace ybdemo');
    client.execute('CREATE KEYSPACE IF NOT EXISTS ybdemo;', next);
  },
  function createTable(next) {
    // The create table statement.
    const create_table = 'CREATE TABLE IF NOT EXISTS ybdemo.employee (id int PRIMARY KEY, ' +
                                                                     'name varchar, ' +
                                                                     'age int, ' +
                                                                     'language varchar, ' +
                                                                     'info jsonb);';
    // Create the table.
    console.log('Creating table employee');
    client.execute(create_table, next);
  },
  function insert(next) {
    // Create a variable with the insert statement.
    const insert = "INSERT INTO ybdemo.employee (id, name, age, language, info) " +
                                        "VALUES (1, 'John', 35, 'NodeJS', '{\"years_experience\": 5}');";
    // Insert a row with the employee data.
    console.log('Inserting row with: %s', insert)
    client.execute(insert, next);
  },
  function select_NOPREPARE_SUCCESS(next) {

    // Query the row for employee id 1 and print the results to the console.
    const select = 'SELECT name, age, language FROM ybdemo.employee WHERE id = 1 AND info->>\'years_experience\' = \'5\';';
    client.execute(select, function (err, result) {
      if (err) return next(err);
      var row = result.first();
      console.log("Unprepared query without bound variables works.");
      console.log('Query for id=1 returned: name=%s, age=%d, language=%s',
                                            row.name, row.age, row.language);
      next();
    });
  },
  function select_PREPARE_SUCCESS(next) {

    // Query the row for employee id 1 and print the results to the console.
    const select = 'SELECT name, age, language FROM ybdemo.employee WHERE id = ? AND info->>\'years_experience\' = \'5\';';
    client.execute(select, [1], {prepare: true}, function (err, result) {
      if (err) return next(err);
      var row = result.first();
      console.log("Prepared query with bound variable (NOT FOR JSONB) works.");
      console.log('Query for id=1 returned: name=%s, age=%d, language=%s',
                                            row.name, row.age, row.language);
      next();
    });
  },
  function select_PREPARE_JSONB_BIND_CRASH(next) {

    // Query the row for employee id 1 and print the results to the console.
    const select = 'SELECT name, age, language FROM ybdemo.employee WHERE id = ? AND info->>\'years_experience\' = ?;';
    client.execute(select, [1, "5"], {prepare: true}, function (err, result) {
      console.log("Prepared query with bound variable (FOR JSONB) crashes the server.");
      if (err) return next(err);
      var row = result.first();
      console.log('Query for id=1 returned: name=%s, age=%d, language=%s',
                                            row.name, row.age, row.language);
      next();
    });
  }
], function (err) {
  if (err) {
    console.error('There was an error', err.message, err.stack);
  }
  console.log('Shutting down');
  client.shutdown();
});