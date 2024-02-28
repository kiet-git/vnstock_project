from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import pickle
import uvicorn

class StockInfo(BaseModel):
    ft_min_6_days: float
    williams_r_3_days: float
    bool_williams_r_3_days: float
    volume: float
    ema_5_days: float
    macd_of_ema3_and_ema5: float
    bool_d_5_days: float
    obv: float
    k_5_days: float
    cci_3_days: float
    std_volume_change_3_day_s_periods: float
    price_change_to_3_day_s_ago: float
    bool_ema_5_days: float
    rsi_5_days: float
    bool_ema_3_days: float
    volume_change_to_3_day_s_ago: float
    ema_3_days: float
    sma_5_days: float
    bool_sma_5_days: float
    d_3_days: float

feature_names = ['ft_min_6_days', 'williams_r_3_days', 'bool_williams_r_3_days',
       'volume', 'ema_5_days', 'macd_of_ema3_and_ema5', 'bool_d_5_days',
       'obv', 'k_5_days', 'cci_3_days', 'std_volume_change_3_day_s_periods',
       'price_change_to_3_day_s_ago', 'bool_ema_5_days', 'rsi_5_days',
       'bool_ema_3_days', 'volume_change_to_3_day_s_ago', 'ema_3_days',
       'sma_5_days', 'bool_sma_5_days', 'd_3_days']

app = FastAPI()
pickle_in = open("stock_classifier.pkl","rb")
classifier = pickle.load(pickle_in)

@app.get("/", response_class=HTMLResponse)
async def home():
    try:
        with open("templates/index.html", "r") as file:
            html_content = file.read()
        return html_content
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="HTML file not found")
    
    return HTMLResponse(content=html_content, status_code=200)

@app.post('/predict')
async def predict_species(data: StockInfo):
    data_dict = data.dict()
    print(data_dict)
    prediction = classifier.predict([[data_dict[feature] for feature in feature_names]])
    if prediction[0] > 0.5:
        prediction_text = "Stock will increase"
    else:
        prediction_text = "Stock will decrease"
    return {"prediction": prediction_text}

if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
