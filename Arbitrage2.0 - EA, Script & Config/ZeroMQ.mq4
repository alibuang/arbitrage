//+------------------------------------------------------------------+
//|                                       ZeroMQ_MT4_EA_Template.mq4 |
//|                                    Copyright 2017, Darwinex Labs |
//|                                        https://www.darwinex.com/ |
//+------------------------------------------------------------------+
#property copyright "Copyright 2017, Darwinex Labs."
#property link      "https://www.darwinex.com/"
#property version   "1.00"
#property strict

// Required: MQL-ZMQ from https://github.com/dingmaotu/mql-zmq
#include <Zmq/Zmq.mqh>

extern string PROJECT_NAME = "DWX_ZeroMQ_Example";
extern string ZEROMQ_PROTOCOL = "tcp";
extern string HOSTNAME = "*";
extern int REP_PORT = 5555;
extern int PUSH_PORT = 5556;
extern int MILLISECOND_TIMER = 1;  // 1 millisecond

extern string t0 = "--- Trading Parameters ---";
extern int MagicNumber = 123456;
extern int MaximumOrders = 1;
extern double MaximumLotSize = 0.01;

// CREATE ZeroMQ Context
Context context(PROJECT_NAME);

// CREATE ZMQ_REP SOCKET
Socket repSocket(context,ZMQ_REP);

// CREATE ZMQ_PUSH SOCKET
Socket pushSocket(context,ZMQ_PUSH);

// VARIABLES FOR LATER
uchar data[];
ZmqMsg request;

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
  {
//---

   EventSetMillisecondTimer(MILLISECOND_TIMER);     // Set Millisecond Timer to get client socket input
   
   Print("[REP] Binding MT4 Server to Socket on Port " + REP_PORT + "..");   
   Print("[PUSH] Binding MT4 Server to Socket on Port " + PUSH_PORT + "..");
   
   repSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, REP_PORT));
   pushSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, PUSH_PORT));
   
   /*
       Maximum amount of time in milliseconds that the thread will try to send messages 
       after its socket has been closed (the default value of -1 means to linger forever):
   */
   
   repSocket.setLinger(1000);  // 1000 milliseconds
   
   /* 
      If we initiate socket.send() without having a corresponding socket draining the queue, 
      we'll eat up memory as the socket just keeps enqueueing messages.
      
      So how many messages do we want ZeroMQ to buffer in RAM before blocking the socket?
   */
   
   repSocket.setSendHighWaterMark(5);     // 5 messages only.
   
//---
   return(INIT_SUCCEEDED);
  }
//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
//---
   Print("[REP] Unbinding MT4 Server from Socket on Port " + REP_PORT + "..");
   repSocket.unbind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, REP_PORT));
   
   Print("[PUSH] Unbinding MT4 Server from Socket on Port " + PUSH_PORT + "..");
   pushSocket.unbind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, PUSH_PORT));
   
}
//+------------------------------------------------------------------+
//| Expert timer function                                            |
//+------------------------------------------------------------------+
void OnTimer()
{
//---

   /*
      For this example, we need:
      1) socket.recv(request,true)
      2) MessageHandler() to process the request
      3) socket.send(reply)
   */
   
   // Get client's response, but don't wait.
   repSocket.recv(request,true);
   
   // MessageHandler() should go here.   
   ZmqMsg reply = MessageHandler(request);
   
   // socket.send(reply) should go here.
   repSocket.send(reply);
}
//+------------------------------------------------------------------+

ZmqMsg MessageHandler(ZmqMsg &request) {
   
   // Output object
   ZmqMsg reply;
   string data2send;
   
   // Message components for later.
   string components[];
   
   if(request.size() > 0) {
   
      // Get data from request   
      ArrayResize(data, request.size());
      request.getData(data);
      string dataStr = CharArrayToString(data);
      
      // Process data
      ParseZmqMessage(dataStr, components);
      
      // Interpret data
      InterpretZmqMessage(&pushSocket, components);
      //Print(dataStr);
      // Construct response
      data2send = StringSubstr(dataStr,0,27);
      ZmqMsg ret(StringFormat("[SERVER]: %s", data2send));
      //Print(StringFormat("[SERVER]: %s", dataStr));
      reply = ret;
      
   }
   else {
      // NO DATA RECEIVED
   }
   
   return(reply);
}

// Interpret Zmq Message and perform actions
void InterpretZmqMessage(Socket &pSocket, string& compArray[]) {

   //Print("ZMQ: Interpreting Message..");
      
   string broker="", comments="", symbol;
   int switch_action = 0, magic_number = 0, count = 0, order_type = 9, err_cd=0, stop_loss=0, take_profit=0, slip=0;
   double open_price=0.0, lot=0.0;
   
   if(compArray[0] == "TRADE" && compArray[1] == "OPEN"){
      switch_action = 1;
      order_type = StringToInteger(compArray[2]);
      symbol = compArray[3];
      open_price = StringToDouble(compArray[4]);
      lot = StringToDouble(compArray[5]);
      stop_loss = StringToInteger(compArray[6]);
      take_profit = StringToInteger(compArray[7]);
      slip = StringToInteger(compArray[8]);
      comments = compArray[9];
      magic_number = StringToInteger(compArray[10]);
   }
   if(compArray[0] == "RATES")
      switch_action = 2;
   if(compArray[0] == "TRADE" && compArray[1] == "CLOSE"){
      switch_action = 3;
      symbol = compArray[2];
      magic_number =StringToInteger(compArray[3]);
      
   }
   if(compArray[0] == "DATA")
      switch_action = 4;
   if(compArray[0] == "COMPANY")
      switch_action = 5;
   if(compArray[0] == "COUNT"){
      switch_action = 6;
      symbol = compArray[1];
      magic_number = compArray[2];
   }
   
   string ret = "";
   int ticket = -1;
   bool ans = FALSE;
   double price_array[];
   ArraySetAsSeries(price_array, true);
   
   
   
   int price_count = 0;
   int space= 0;
   
   switch(switch_action) 
   {
      case 1: 
         
         err_cd = send_Order(order_type, symbol, open_price, lot ,stop_loss, take_profit, slip, comments, magic_number, 101);
         ret = StringFormat("%d", err_cd);
                  
         InformPullClient(pSocket, ret);
        // InformPullClient(pSocket,"OPEN TRADE Instruction Received");
         // IMPLEMENT OPEN TRADE LOGIC HERE
         break;
      case 2: 
         ret = "N/A"; 
         if(ArraySize(compArray) > 1) 
            ret = GetBidAsk(compArray[1]); 
            
         InformPullClient(pSocket, ret); 
         break;
      case 3:
        // InformPullClient(pSocket,"CLOSE TRADE Instruction Received");
         
         err_cd = CloseAll(symbol, magic_number);
         
         ret = StringFormat("%d", err_cd);
         InformPullClient(pSocket, ret);
         
         break;
      
      case 4:
         InformPullClient(pSocket, "HISTORICAL DATA Instruction Received");
         
         // Format: DATA|SYMBOL|TIMEFRAME|START_DATETIME|END_DATETIME
         price_count = CopyClose(compArray[1], StrToInteger(compArray[2]), 
                        StrToTime(compArray[3]), StrToTime(compArray[4]), 
                        price_array);
         
         if (price_count > 0) {
            
            ret = "";
            
            // Construct string of price|price|price|.. etc and send to PULL client.
            for(int i = 0; i < price_count; i++ ) {
               
               if(i == 0)
                  ret = compArray[1] + "|" + DoubleToStr(price_array[i], 5);
               else if(i > 0) {
                  ret = ret + "|" + DoubleToStr(price_array[i], 5);
               }   
            }
            
            Print("Sending: " + ret);
            
            // Send data to PULL client.
            InformPullClient(pSocket, StringFormat("%s", ret));
            // ret = "";
         }
            
         break;
         
      case 5: 
         space = StringFind(AccountCompany()," ",0);
         broker = StringSubstr(AccountCompany(),0, space);
         InformPullClient(pSocket, broker);
         
         break;
         
      case 6:
         count = CountTrades(symbol, magic_number);
         InformPullClient(pSocket, count);
         break;
         
      default: 
         break;
   }
}

// Parse Zmq Message
void ParseZmqMessage(string& message, string& retArray[]) {
   
   //Print("Parsing: " + message);
   
   string sep = "|";
   ushort u_sep = StringGetCharacter(sep,0);
   
   int splits = StringSplit(message, u_sep, retArray);
   
   //for(int i = 0; i < splits; i++) {
   //   Print(i + ") " + retArray[i]);
   //}
}

//+------------------------------------------------------------------+
// Generate string for Bid/Ask by symbol
string GetBidAsk(string symbol) {

   datetime timestamp = TimeLocal();
   string time_str = TimeToStr(timestamp,TIME_DATE|TIME_SECONDS);
   string ret_string;
   
   double bid = MarketInfo(symbol, MODE_BID);
   double ask = MarketInfo(symbol, MODE_ASK);
   double spread = MarketInfo(symbol, MODE_SPREAD);
   double digits = MarketInfo(symbol, MODE_DIGITS);
   
   ret_string = StringFormat("%s|%f|%f|%f|%f",time_str, bid, ask, spread, digits);
   //Print(ret_string);
   return(ret_string);
}

// Inform Client
void InformPullClient(Socket& pushSocket, string message) {

   ZmqMsg pushReply(StringFormat("%s", message));
   pushSocket.send(pushReply,true,false);
   
}

//+------------------------------------------------------------------+
//|   This function is to Count number of trades                                                               |
//+------------------------------------------------------------------+
int CountTrades(string symbol, int magic_number)
{
            int count=0;
            int trade;
            for(trade=OrdersTotal()-1;trade>=0;trade--)
              {
               int order_chk = OrderSelect(trade,SELECT_BY_POS,MODE_TRADES);
               if(OrderSymbol()!=symbol ||OrderMagicNumber()!=magic_number)
                  continue;
               if(OrderSymbol()==symbol &&OrderMagicNumber()==magic_number)
                  if(OrderType()==OP_SELL || OrderType()==OP_BUY)
                     count++;
              }//for
            return(StringFormat("%d",count));
}

//+----------------------------------------------------------------
// This function is to execute buy command
//+----------------------------------------------------------------
bool send_Order(int order_type,string iSymbol, double iOpenPrice, double iLot,int iStopLoss, int iTakeProfit, int iSlip, string comments, int iMagicNumber, int err_type){

   bool order_status = false;
   int ticket,retry=5;
   double lot= iLot;
   double open_price=iOpenPrice;
   double stop_loss=0.0;
   double take_profit=0.0;
   int slip=iSlip;
   string comment_level=comments;
   string str_order_type, err_msg, text_msg;
   
     
   //-----------------------------------------------
   RefreshRates();
   double ask_price=NormalizeDouble(MarketInfo(iSymbol,MODE_ASK),Digits);
   double bid_price=NormalizeDouble(MarketInfo(iSymbol,MODE_BID),Digits);
   int spread_price=MarketInfo(Symbol(),MODE_SPREAD);
      
   //------------------------------------------------

  // take_profit = NormalizeDouble(order_TP,Digits);  
   
   if(order_type == OP_BUY ){
      str_order_type = "Buy";
     // open_price = ask_price;
      if(iStopLoss != 0) stop_loss = open_price - (iStopLoss * Point); 
      if(iTakeProfit != 0) take_profit = open_price + (iTakeProfit * Point);      
   }    
   else if(order_type == OP_SELL){
      str_order_type = "Sell"; 
     // open_price = bid_price;
      if(iStopLoss != 0) stop_loss = open_price + (iStopLoss * Point);  
      if(iTakeProfit != 0) take_profit = open_price - (iTakeProfit * Point);     
   }
   
    
   for(int i=0; i<retry; i++)
   {
      ticket=OrderSend(iSymbol,order_type,lot,open_price,slip,stop_loss,take_profit,comment_level,iMagicNumber,0,CLR_NONE);
      //PlaySound("expert.wav");
      if(ticket<0){
         err_msg = "OrderSend failed with error #" + GetLastError();
         Print(err_msg); 
         write_error_file(err_msg);
            
         err_msg = "Line375|send_Order|OrderType="+str_order_type+"|Err_type="+ err_type+"|lot="+lot
                  +"|Price=" +open_price + "|TakeProfit="+ take_profit + "|StopLoss="+stop_loss +"|Spread="+spread_price
                  +"|Bid="+bid_price+"|Ask="+ask_price+"|Market_Bid="+MarketInfo(Symbol(),MODE_BID)+"|Market_Ask="+MarketInfo(Symbol(),MODE_ASK);         
         Print(err_msg); 
         write_error_file(err_msg);
      }
      else{
         order_status = true;
         
                  
         break;
      }
   }
   Sleep(1000);
   return order_status;
  }
//+----------------------------------------------------------------
// END
//+----------------------------------------------------------------

//---------------------------------------------------------
// This function is to close all open position
//---------------------------------------------------------
bool CloseAll(string symbol, int magic_number){

   bool closed=false;
   int order_type;
   int slip = 1;
   bool err_flag;
   int total_order = OrdersTotal();
   string err_msg;
   
  // Print("Order Total=",total_order);
   
   for (int i = total_order - 1; i >= 0; i--) {  //#
         OrderSelect (i, SELECT_BY_POS, MODE_TRADES);
      
         if (OrderMagicNumber()== magic_number && OrderSymbol()==symbol&&(OrderType()== OP_BUY||OrderType()==OP_SELL)) 
         {
            order_type = OrderType();
            err_flag = false;
            //retry loop if there is error to close position
            
               for(int j = 3; j>0 ;j--){   
                     RefreshRates();           
                    
                      if( order_type == OP_BUY){
                           err_flag = OrderClose(OrderTicket(), OrderLots(), MarketInfo(OrderSymbol(), MODE_BID), slip, CLR_NONE);
                           if(err_flag) {
                              closed = true;
                              break;
                              
                           }
                      }
                      if(order_type == OP_SELL){
                           err_flag = OrderClose(OrderTicket(), OrderLots(), MarketInfo(OrderSymbol(), MODE_ASK), slip, CLR_NONE);
                           if(err_flag) {
                              closed = true;
                              break;
                              
                           }
                      }              
                     else if(err_flag == false){
                           err_msg = "OrderClose failed with error #" + GetLastError();
                           Print(err_msg); 
                           write_error_file(err_msg);
                           
                           err_msg = "Line546|CloseAll()|OrderTicket="+OrderTicket()+"|OrderLots="+OrderLots();
                           Print(err_msg); 
                           write_error_file(err_msg);                          
                     }       
               }                      
            }                
    } 
    
    return closed;
    
}

//+----------------------------------------------------------------
// END
//+----------------------------------------------------------------

//-------------------------------------------------
// This function is to log error into filr
//--------------------------------------------------
bool write_error_file (string error_msg){
   
   bool write_status = false;
   ulong pos[];
   
   string InpFileName = "error_msg.log";
   
   string time_log = TimeToString(TimeLocal(),TIME_DATE|TIME_SECONDS);
   string pair_log = Symbol();
   string error_log = time_log+","+pair_log+","+error_msg;
   
   //--- reset error value 
   ResetLastError(); 
   //--- open the file 
   int file_handle=FileOpen(InpFileName,FILE_READ|FILE_WRITE|FILE_TXT); 
   if(file_handle!=INVALID_HANDLE) 
     { 
         FileSeek(file_handle,0,SEEK_END);
         FileWrite(file_handle,error_log);
      
      
          //--- close the file
         FileClose(file_handle);
        // PrintFormat("%s file is closed",InpFileName);    
         
     } 
   else {
      PrintFormat("Error, code = %d",GetLastError()); 
      write_status = false;
      }
   
   return write_status;
}