Please run "python server.py" in terminal 1, and then run "python client.py" in terminal 2.

The exchange server will be started in terminal 1. The clients will be connected to it. There will be confirmations of connection on both
the server and the client sides.

Then there will be a pause for 5 sec before trading starts.

In terminal 2, it will be shown what messages are sent and received by the traders. Each trader waits 1 sec after sending a message.

In terminal 1, it will be shown what messages are received from the traders, and what messages are sent to the traders.

Also, in terminal 1, every 5 seconds, a line showing the sum of balances of all the traders, and also the individual balances of the
traders, will be printed. When it is printed, the trading is suspended for 2 seconds, so that it is possible to read the line.

Use Ctrl+C to stop the server in terminal 1.

![Demo](stock-exchange-demo.gif)
