/* eslint-disable padded-blocks */

const chai = require('chai');
const mysql = require('mysql');
const MySQLEvents = require('./src');

before(() => {
  chai.should();
});

const delay = (timeout = 1000) => new Promise((resolve) => {
  setTimeout(resolve, timeout);
});

describe('MySQLEvents', () => {

  it('should connect and disconnect from MySQL', async () => {
    const connection = mysql.createConnection({
      host: '10.3.250.49',
      user: 'root',
      password: '021zaq#@!inq',
    });

    const instance = new MySQLEvents(connection);

    await instance.start();

    await delay();

    await instance.stop();
  }).timeout(2000);

});