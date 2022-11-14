const amqplib = require('amqplib');
const publisher  = require('./publisher');

const AMQP_URL = process.env.AMQP_URL;
const AMQP_QUEUE = process.env.AMQP_QUEUE;

async function processMessage(msg, channel, callback) {

  try {

    let payload = JSON.parse( msg.content.toString() );  
    
	let payload_retorno = (callback) ? await callback( payload ) : null;
    
    if ( process.env.AMQP_WEBHOOK_QUEUE && payload_retorno ){
         let pubret = await publisher(process.env.AMQP_WEBHOOK_QUEUE, payload_retorno );
    }

    if ( process.env.AMQP_QUEUE_PUBLISHER && payload_retorno ){
        let pubret = await publisher(process.env.AMQP_QUEUE_PUBLISHER, payload_retorno );         
    }

    await channel.ack(msg);    
    
  } catch (error) {
    console.log( 'error', error );
    await channel.ack(msg);    
  }

}

module.exports.publisher = publisher;

module.exports.worker = async (QUEUE = AMQP_QUEUE,callback) => {

    const connection = await amqplib.connect(AMQP_URL, "heartbeat=60");
    const channel = await connection.createChannel();

    channel.prefetch(10);    
    process.once('SIGINT', async () => { 
      console.log('got sigint, closing connection');
      await channel.close();
      await connection.close(); 
      process.exit(0);
    });

    await channel.assertQueue(QUEUE, {durable: true});
    
    await channel.consume(QUEUE, 
      async (payload) => {	
        return await processMessage(payload, channel, callback);        
      },{
      noAck: false/*,
      consumerTag: 'email_consumer'*/
    });
    
    console.log(" [*] Waiting for jobs... ");


};