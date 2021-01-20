import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

import datetime as dt

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///../Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate App!<br/>"
        f"___________________________<br/>"
        f"Available Routes:<br/>"
        f"  /api/v1.0/precipitation<br/>"
        f"  /api/v1.0/stations<br/>"
        f"  /api/v1.0/tobs<br/>"
        f"  /api/v1.0/start_date=YYY-MM-DD/<br/>"
        f"  /api/v1.0/start_date=YYY-MM-DD/end_date=YYY-MM-DD"
    )

#API Routes
@app.route("/api/v1.0/precipitation")
def percipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of prcp last 12 months"""
    # Query tables
    last_12months_date = dt.datetime.strptime(session.query(Measurement).\
                    order_by(Measurement.date.desc()).\
                    first().date,'%Y-%m-%d')-dt.timedelta(days=366)

    percip_results = session.query(Measurement.station, Measurement.date, Measurement.prcp).\
                    filter(Measurement.date > last_12months_date).\
                    order_by(Measurement.date.asc()).all()

    session.close()

    # Create a dictionary from the row data and append to a list 
    all_prcp = []
    
    for station, date, prcp in percip_results:
     all_prcp_dict = {}
     all_prcp_dict["station"] = station
     all_prcp_dict["date"] = date
     all_prcp_dict["prcp"] = prcp
     all_prcp.append(all_prcp_dict)

    return jsonify(all_prcp)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    """Return a JSON list of stations from the dataset"""
    # Convert list of tuples into normal list
    station_results = session.query(Station.name).all()
    
    session.close()
    
    all_stations = list(np.ravel(station_results))
    
    return jsonify(all_stations)

#API Route percipitation
@app.route("/api/v1.0/tobs")
def tobs_most_active_station():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of prcp last 12 months"""
    # Query tables
    most_active_station = session.query(Measurement.station , func.count(Station.station).label("records")).\
                                group_by(Measurement.station).\
                                order_by(func.count(Station.station).desc()).first()

    last_12months_date = dt.datetime.strptime(session.query(Measurement).\
                    order_by(Measurement.date.desc()).\
                    first().date,'%Y-%m-%d')-dt.timedelta(days=366)

    most_active_station_lst12mnths =session.query(Measurement.station,Measurement.date,Measurement.tobs,).\
                    filter(Measurement.station == most_active_station[0]).\
                    filter(Measurement.date >= last_12months_date).\
                    order_by(Measurement.date.desc()).all()
    session.close()

    # Create a dictionary from the row data and append to a list 
    active_temp = []

    for station, date, tobs in most_active_station_lst12mnths:
     act_temp_dict = {}
     #act_temp_dict["station"] = station
     act_temp_dict["date"] = date
     act_temp_dict["temp"] = tobs
     active_temp.append(act_temp_dict)

    return jsonify(active_temp)

@app.route("/api/v1.0/<start_dt>", methods=['GET'])
def start_date(start_dt):
    session = Session(engine)
    """start date to limit data pulled"""
    result_temp = session.query(func.max(Measurement.tobs).label("max_temp"),\
    func.min(Measurement.tobs).label("min_temp"),\
    func.avg(Measurement.tobs).label("avg_temp")).\
    filter(Measurement.date >= start_dt).all()
 
    session.close()

    temp_results = []

    for max_temp, min_temp, avg_temp in result_temp:
      temp_dict = {}
      temp_dict["max_temp"] = max_temp
      temp_dict["min_temp"] = min_temp
      temp_dict["avg_temp"] = round(avg_temp,1)
      temp_results.append(temp_dict)

    return jsonify(temp_results)

@app.route("/api/v1.0/<start_dt>/<end_dt>", methods=['GET'])
def range_dates(start_dt,end_dt):
    session = Session(engine)
    """start date to limit data pulled"""
    result_temp_range = session.query(func.max(Measurement.tobs).label("max_temp"),\
    func.min(Measurement.tobs).label("min_temp"),\
    func.avg(Measurement.tobs).label("avg_temp")).\
    filter(Measurement.date >= start_dt).\
    filter(Measurement.date <= end_dt).all()
 
    session.close()

    temp_results_range = []

    for max_temp, min_temp, avg_temp in result_temp_range:
      temp_dtrng_dict = {}
      temp_dtrng_dict["max_temp"] = max_temp
      temp_dtrng_dict["min_temp"] = min_temp
      temp_dtrng_dict["avg_temp"] = round(avg_temp,1)
      temp_results_range.append(temp_dtrng_dict)

    return jsonify(temp_results_range)

if __name__ == '__main__':
    app.run(debug=True)