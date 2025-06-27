const connection = await amqp.connect(
      process.env.URL_RABBIT_MQ
    );
    const channel = await connection.createChannel();

    await channel.assertQueue("rel_listas_queue", { durable: true });
    channel.sendToQueue("rel_listas_queue", Buffer.from(JSON.stringify(message)));

    await channel.close();
    await connection.close();
