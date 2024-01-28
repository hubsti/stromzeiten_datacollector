from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from keras.layers import Dense, LSTM
from keras.models import Sequential
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.seasonal import seasonal_decompose

# Simulating data for the last 10 days
np.random.seed(0)
dates = pd.date_range(end=datetime.today(), periods=10).tolist()
carbon_emissions = np.random.uniform(
    100, 200, size=10
)  # Random carbon emissions values
temperature = np.random.uniform(-5, 30, size=10)  # Simulated temperature values
humidity = np.random.uniform(30, 90, size=10)  # Simulated humidity values
wind_speed = np.random.uniform(0, 20, size=10)  # Simulated wind speed values

# Creating a DataFrame
data = pd.DataFrame(
    {
        "Date": dates,
        "Carbon_Emissions": carbon_emissions,
        "Temperature": temperature,
        "Humidity": humidity,
        "Wind_Speed": wind_speed,
    }
)

data.set_index("Date", inplace=True)
data.head(10)  # Displaying the simulated dataset


# Preprocessing - Scaling the features
scaler = MinMaxScaler(feature_range=(0, 1))
scaled_data = scaler.fit_transform(data)

# Splitting the data into training and test sets
# We use the first 7 days for training and the last 3 days for testing
train_data = scaled_data[:-3]
test_data = scaled_data[-7:]  # Last 7 days for testing, including the 3 days to predict

# Preparing the data for LSTM - reshaping into [samples, time steps, features]
train_X, train_y = train_data[:, 1:], train_data[:, 0]
test_X, test_y = test_data[:, 1:], test_data[:, 0]

train_X = train_X.reshape((train_X.shape[0], 1, train_X.shape[1]))
test_X = test_X.reshape((test_X.shape[0], 1, test_X.shape[1]))

# Defining the LSTM model
model = Sequential()
model.add(LSTM(50, input_shape=(train_X.shape[1], train_X.shape[2])))
model.add(Dense(1))
model.compile(loss="mean_squared_error", optimizer="adam")

# Training the model (simulated)
# Normally, we would fit the model, but here we simulate this step
model.fit(train_X, train_y, epochs=50, batch_size=1, verbose=2)

# Making predictions (simulated)
test_predictions = model.predict(test_X)

# Simulating SARIMA model for seasonality (not actually implementing due to complexity)
import matplotlib.pyplot as plt

# Decomposing the series to understand its components
decomposition = seasonal_decompose(data["Carbon_Emissions"], model="additive", period=1)
decomposition.plot()
plt.show()

# SARIMA Model Configuration
# Note: The parameters (p, d, q) and (P, D, Q, s) would normally be determined after analysis
# We will use a basic configuration here
p, d, q = 1, 1, 1  # AR, differencing, and MA parameters for non-seasonal component
P, D, Q, s = (
    1,
    1,
    1,
    7,
)  # Seasonal AR, differencing, and MA parameters, and seasonal period

# Fitting the SARIMA model
sarima_model = SARIMAX(
    data["Carbon_Emissions"], order=(p, d, q), seasonal_order=(P, D, Q, s)
)
sarima_results = sarima_model.fit()

# Display the summary of the model
sarima_summary = sarima_results.summary()
sarima_summary
final_predictions = (test_predictions) / 2

# Simulated RMSE (as we're not actually training the model)
simulated_rmse = np.sqrt(
    mean_squared_error(test_y, np.random.uniform(0, 1, len(test_y)))
)
simulated_rmse
