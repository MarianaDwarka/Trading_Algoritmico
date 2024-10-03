import pandas as pd
import numpy as np
import tpqoa
from datetime import datetime, timedelta
import time
import pytz
from sqlalchemy import create_engine, Table, MetaData, insert

# Establecemos los datos de conexión ('mysql+pymysql://usuario:contraseña@host/nombre_BBDD)
sqlEngine = create_engine('mysql+pymysql://root:12345abcd@localhost/trading', pool_recycle=3600)
# Establecemos la conexión
dbConnection = sqlEngine.connect()

from sqlalchemy.orm import sessionmaker

# Crear una sesión
Session = sessionmaker(bind=sqlEngine)
session = Session()
metadata = MetaData()

class SMATrader(tpqoa.tpqoa):
    def __init__(self, conf_file, instrument, bar_length, SMA_S, SMA_L, units, tablanom= 'trading_aut'):
        super().__init__(conf_file)
        self.instrument = instrument
        self.bar_length = pd.to_timedelta(bar_length)
        self.tick_data = pd.DataFrame()
        self.raw_data = None
        self.data = None 
        self.last_bar = None
        self.units = units
        self.position = 0
        self.profits = []
        
        #*****************ATRIBUTOS DE LA ESTRATEGIA******************
        self.SMA_S = SMA_S
        self.SMA_L = SMA_L
        #************************************************************************

        self.tablanom = tablanom
    
    def get_most_recent(self, days = 5):
        while True:
            time.sleep(2)
            now = datetime.utcnow()
            now = now - timedelta(microseconds = now.microsecond)
            past = now - timedelta(days = days)
            df = self.get_history(instrument = self.instrument, start = past, end = now,
                                   granularity = "S5", price = "M", localize = False).c.dropna().to_frame()
            df.rename(columns = {"c":self.instrument}, inplace = True)
            df = df.resample(self.bar_length, label = "right").last().dropna().iloc[:-1]
            self.define_strategy(df)
            self.raw_data = df.tail(250).copy()

            #************ CONEXIÓN CON LA BASE DE DATOS ***************
            # Damos nombre a la tabla que vamos a crear
            try:
                # Creamos la tabla en la BBDD con los datos del dataFrame
                frame = df.to_sql(self.tablanom, dbConnection, if_exists='fail');
            
            except ValueError as vx:
                print(vx)
            except Exception as ex:   
                print(ex)
            else:
                # Si no da error, imprime que se ha creado correctamente
                print("La tabla se ha creado exitosamente.");   
                
            #********************************************************

            self.last_bar = self.raw_data.index[-1]
            if pd.to_datetime(datetime.utcnow()).tz_localize("UTC") - self.last_bar < self.bar_length:
                break
                
    def on_success(self, time, bid, ask):
        print("Numero: {}, Time: {} | Bid: {} | Ask:{} | Mid:{} ".format(self.ticks,time, bid, ask, (ask + bid)/2))
        #print(self.ticks, end = " ")
        
        recent_tick = pd.to_datetime(time)

        #************ AQUI SE DEFINE LA FECHA EN QUE FINALIZA LA EJECUCIÓN 
        if recent_tick >= pd.to_datetime("2024-10-29 22:16").tz_localize(pytz.UTC):
            self.stop_stream = True
        #***********************************************

        df = pd.DataFrame({self.instrument:(ask + bid)/2}, 
                          index = [recent_tick])
        self.tick_data = pd.concat([self.tick_data, df]) # new with pd.concat()
        
        if recent_tick - self.last_bar > self.bar_length:
            self.resample_and_join()
            self.define_strategy(self.raw_data)

            #****************
            try:
                ultimo_registro = self.raw_data.iloc[-1].to_dict()
                ultimo_registro['time'] = df.index[-1].strftime('%Y-%m-%d %H:%M:%S')
                print('ultimo: ', ultimo_registro)
                
                tabla = Table( self.tablanom, metadata, autoload_with=sqlEngine)
                session.execute(tabla.insert(), ultimo_registro)
                session.commit()
            except ValueError as vx:
                print(vx)
            except Exception as ex:   
                print(ex)
            else:
                # Si no da error, imprime que se ha creado correctamente
                print('Se ha cargado el registro');   
                #****************
            
            self.data = self.raw_data.copy()
            self.execute_trades()
    
    def resample_and_join(self):
        self.raw_data = pd.concat([self.raw_data, self.tick_data.resample(self.bar_length, 
                                                                          label="right").last().ffill().iloc[:-1]])
        self.tick_data = self.tick_data.iloc[-1:]
        self.last_bar = self.raw_data.index[-1]
    
    def define_strategy(self, df): # "Estrategia específica"
        
        #******************** se define la estrategia ************************
        df["SMA_S"] = df[self.instrument].rolling(self.SMA_S).mean()
        df["SMA_L"] = df[self.instrument].rolling(self.SMA_L).mean()
        df["position"] = np.where(df["SMA_S"] > df["SMA_L"], 1, -1)
        #***********************************************************************
        
    
    def execute_trades(self):
        if self.data["position"].iloc[-1] == 1:
            if self.position == 0:
                order = self.create_order(self.instrument, self.units, suppress = True, ret = True)
                self.report_trade(order, "GOING LONG")
            elif self.position == -1:
                order = self.create_order(self.instrument, self.units * 2, suppress = True, ret = True) 
                self.report_trade(order, "GOING LONG")
            self.position = 1
        elif self.data["position"].iloc[-1] == -1: 
            if self.position == 0:
                order = self.create_order(self.instrument, -self.units, suppress = True, ret = True)
                self.report_trade(order, "GOING SHORT")
            elif self.position == 1:
                order = self.create_order(self.instrument, -self.units * 2, suppress = True, ret = True)
                self.report_trade(order, "GOING SHORT")
            self.position = -1
        elif self.data["position"].iloc[-1] == 0: 
            if self.position == -1:
                order = self.create_order(self.instrument, self.units, suppress = True, ret = True)
                self.report_trade(order, "GOING NEUTRAL")
            elif self.position == 1:
                order = self.create_order(self.instrument, -self.units, suppress = True, ret = True) 
                self.report_trade(order, "GOING NEUTRAL")
            self.position = 0
    
    def report_trade(self, order, going):
        time = order["time"]
        units = order["units"]
        price = order["price"]
        pl = float(order["pl"])
        self.profits.append(pl)
        cumpl = sum(self.profits)
        print("\n" + 100* "-")
        print("{} | {}".format(time, going))
        print("{} | units = {} | price = {} | P&L = {} | Cum P&L = {}".format(time, units, price, pl, cumpl))
        print(100 * "-" + "\n")  


if __name__ == "__main__":
    print('Hola')
    trader = SMATrader("oanda.cfg", "EUR_USD", "1min", SMA_S = 30, SMA_L = 220, units = 100000) #1h
    trader.get_most_recent()
    trader.stream_data(trader.instrument) 
    if trader.position != 0: # if we have a final open position
        close_order = trader.create_order(trader.instrument, units = -trader.position * trader.units, 
                                        suppress = True, ret = True) 
        trader.report_trade(close_order, "GOING NEUTRAL")
        trader.position = 0