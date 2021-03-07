# =========================================================
# Import libraries needed
# =========================================================
import pandas as pd
import datetime as dt
from datetime import timedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.sql import label

from flask import Flask, jsonify

# =========================================================
# Database Setup
# =========================================================
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

# =========================================================
# Flask Setup & Routes
# =========================================================

# Flask Setup
app = Flask(__name__)

# Home Route
@app.route('/')
def home():
    return (
        f'Availible Routes:<br>'
        f'/api/v1.0/precipitation<br>'
        f'/api/v1.0/stations<br>'
        f'/api/v1.0/tobs<br>'
        f'/api/v1.0/&lt;start&gt;<br>'
        f'/api/v1.0/&lt;start&gt;/&lt;end&gt;'
        )

# Precipitation Route
@app.route('/api/v1.0/precipitation')
def precipitation():
    precipitation = dict(session.query(Measurement.date, Measurement.prcp).all())
    return jsonify(precipitation)

# Flask Route: Stations
@app.route('/api/v1.0/stations')
def stations():
    station = dict(session.query(Station.station, Station.name).all())
    return jsonify(station)

# Flask Route: Temperature
@app.route('/api/v1.0/tobs')
def tobs():
    # Find the most active station
    stations = pd.DataFrame(session.query(Measurement.station, label('row_ct', func.count(Measurement.station))).group_by(Measurement.station).all())
    top_station = stations['station'][stations['row_ct'] == stations['row_ct'].max()].item()

    
    # Find the most recent date in the data set.
    max_date = session.query(func.max(Measurement.date)).scalar()
    
    # Starting from the most recent data point in the database. 
    endate = dt.datetime.strptime(max_date, '%Y-%m-%d').date()
    
    # Calculate the date one year from the last date in data set.
    startdate = endate - timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    temps = dict(session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= startdate, Measurement.date <= endate, Measurement.station == top_station).all())

    return jsonify(temps)

# Flask Route: Start
@app.route('/api/v1.0/<start>')
def tobs_start(start):
    # Perform a query to retrieve the data and precipitation scores
    temps = pd.DataFrame(session.query(label('TMIN', func.min(Measurement.tobs))
                                        , label('TAVG', func.avg(Measurement.tobs))
                                        , label('TMAX', func.max(Measurement.tobs))
                                        ).filter(Measurement.date >= start).all())
    temps = temps.to_dict('records')[0]
    return jsonify(temps)

# Flask Route: Start & End
@app.route('/api/v1.0/<start>/<end>')
def tobs_start_end(start,end):
    # Perform a query to retrieve the data and precipitation scores
    temps = pd.DataFrame(session.query(label('TMIN', func.min(Measurement.tobs))
                                        , label('TAVG', func.avg(Measurement.tobs))
                                        , label('TMAX', func.max(Measurement.tobs))
                                        ).filter(Measurement.date >= start, Measurement.date <= end).all())
    temps = temps.to_dict('records')[0]
    return jsonify(temps)

# Close Session
session.close()

if __name__ == "__main__":
    app.run(debug=True)