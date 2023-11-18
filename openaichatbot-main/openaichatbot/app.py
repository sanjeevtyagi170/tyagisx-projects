import config
import openai
import json
import pandas as pd
from flask import Flask,request,render_template

app = Flask(__name__)
app.debug = True

def get_completion_from_messages(messages, model="gpt-3.5-turbo", temperature=0):
    '''Function to call an openai api properly'''
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=temperature, # this is the degree of randomness of the model's output
    )
#     print(str(response.choices[0].message))
    return response.choices[0].message["content"]
context = [ {'role':'system', 'content':"""
    You are an OrderBot, an automated service to collect orders for an Indian food restaurant. \
    You first greet the customer at the beginning, and then collects the order, \
    Please clarify the items from the menu and do not take the order for an item if an item does not present in the below food menu\
    and then asks if it's a pickup or delivery. \
    You wait to collect the entire order, then summarize it and check for a final \
    time if the customer wants to add anything else. \
    If it's a delivery, you ask for an address. \
    Finally you collect the payment.\
    Make sure to clarify all options, extras and sizes to uniquely \
    identify the item from the menu.\
    please ask for name and phone number. And always inlcude keyword 'Goodbye' at the end of the conversation\
    You respond in a short, very conversational friendly style. \
    The menu includes:
    Main Course
    - Dal Makhani: ₹100, ₹200
    - Shahi Paneer: ₹120, ₹240
    Breads:
    - Butter naan: ₹2.5
    - Tawa Roti: ₹3.5
    Snacks:
    - Pizza: ₹12.95, ₹10.00, ₹7.00
    - Fries: ₹4.50, ₹3.50
    - Burger: ₹7.25, ₹9.25
    Drinks:
    - coke: ₹3.00, ₹2.00, ₹1.00
    - sprite: ₹3.00, ₹2.00, ₹1.00
    - bottled water: ₹5.00
    """} ]  # accumulate messages

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    user_input = request.form['user_input']
    context.append({'role': 'user', 'content': user_input})
    response = get_completion_from_messages(context)
    context.append({'role': 'assistant', 'content': response})
    chat_history = [message['content'] for message in context[1:]]
    if "bye" in user_input.lower() or "goodbye" in response.lower():
        messages =  context.copy()
        messages.append(
        {
        'role':'system', 
        'content':'create a json summary of the previous food order. Itemize the price for each item\
        The fields should be \
        1) total price\
        2) name\
        3) phone\
        4) address \
        5) mode of payment'},    
        )

        response = get_completion_from_messages(messages, temperature=0)
        # --------------------- Appending Data----------------#
        data = json.loads(response)
        order=pd.json_normalize(data)
        try:
            previous_orders=pd.read_csv('orders.csv')
        except pd.errors.EmptyDataError:
            order.to_csv('orders.csv',index=False,header=True)
        else:
            order=pd.concat([previous_orders,order],axis=0)
            order.to_csv('orders.csv',mode='w',header=True,index=False)
        return render_template('index.html', chat_history=chat_history, end_chat=True)
    else:
        return render_template('index.html', chat_history=chat_history, end_chat=False)


if __name__ == '__main__':
    app.run()


# I want to order one dal makhani with butter naan and pepsi. This order is for pickup at 3 pm today. my name is sanjeev and phone number is 8470011123