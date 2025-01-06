# import os
# from sendgrid import SendGridAPIClient
# from sendgrid.helpers.mail import Mail

# # Define sender and recipient email addresses and the API key
# from_email = "dhanupriya@goleads.com"  # Change to your recipient
# to_email = ["dhanupriyademootk@gmail.com","dhanupriyaworkid@gmail.com"] 

# SENDGRID_API_KEY = "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"  # Replace with your SendGrid API key

# # Create the email content
# message = Mail(
#     from_email=from_email,
#     to_emails=to_email,
#     subject="Sending with Twilio SendGrid is Fun",
#     html_content='<strong>and easy to do anywhere, even with Python</strong>'
# )

# # Initialize the SendGrid client with the API key
# sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
# try:
#     response = sg.send(message)
#     print("Email sent successfully!")
# except Exception as e:
#     print("Failed to send email!")
#     print(e)


import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Replace with your SendGrid API key
SENDGRID_API_KEY = "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"  

# Read email addresses from CSV file
def read_email_addresses_from_csv(csv_file):
    df = pd.read_csv(csv_file)
    email_addresses = df['email'].tolist()  # Assuming 'email' is the column name containing email addresses
    return email_addresses

# Create the email content
def create_email_message(from_email, to_emails, subject, html_content):
    message = Mail(
        from_email=from_email,
        to_emails=to_emails,
        subject=subject,
        html_content=html_content
    )
    return message

# Send emails
def send_emails(sendgrid_client, message):
    try:
        response = sendgrid_client.send(message)
        print("Email sent successfully!")
    except Exception as e:
        print("Failed to send email!")
        print(e)

if __name__ == "__main__":
    from_email = "dhanupriya@goleads.com"  # Change to your sender email
    csv_file = "testmail.csv"  # Change to your CSV file path

    # Read email addresses from CSV file
    to_emails = read_email_addresses_from_csv(csv_file)

    # Initialize the SendGrid client with the API key
    sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)

    # Create and send email for each recipient
    for email in to_emails:
        message = create_email_message(from_email, email, "Sending with Twilio SendGrid is Fun", '<strong>and easy to do anywhere, even with Python</strong>')
        send_emails(sg, message)


