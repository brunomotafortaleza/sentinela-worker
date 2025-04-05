const AMQP_URL = process.env.AMQP_URL;

const amqplib = require('amqplib');
const amqpUrl = AMQP_URL;

module.exports = async (AMQP_QUEUE, msgs = null) => {
  
  if (!msgs || msgs.length == 0) return;

  const connection = await amqplib.connect(amqpUrl, 'heartbeat=60');
  const channel = await connection.createChannel();

  var rets = [];

  try {
    
    const exchange = AMQP_QUEUE+'_exchange';
    const queue = AMQP_QUEUE;
    const routingKey = AMQP_QUEUE+'_key';
    
    await channel.assertExchange(exchange, 'direct', {durable: true});
    await channel.assertQueue(queue, {
      durable: true,
      autoDelete: false
    });
    await channel.bindQueue(queue, exchange, routingKey);		
		
    //converte sempre em array
    msgs = ( Array.isArray( msgs ) ) ? msgs : [msgs];    

    for (let msg of msgs) {
      let ret = await channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(msg)), {persistent: true});  
      rets.push(ret);
    };

    console.log( rets );

    
    
  } catch(e) {

    rets = e

  } finally { 

    await channel.close();
    await connection.close();

    return rets;
    
  }
  
}
