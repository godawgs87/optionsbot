"""
Helper script to debug Telegram bot configuration.
"""
import os
import requests
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def test_telegram_bot():
    """Test Telegram bot token and chat ID."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token:
        logger.error("No Telegram bot token found in environment variables")
        return False
    
    if not chat_id:
        logger.error("No Telegram chat ID found in environment variables")
        return False
    
    # Test the bot token
    try:
        response = requests.get(f"https://api.telegram.org/bot{token}/getMe")
        if response.status_code != 200:
            logger.error(f"Invalid bot token. Response: {response.text}")
            return False
        
        bot_info = response.json()
        logger.info(f"Bot information: {bot_info}")
        
        # Test sending a message to the chat
        message_text = "🔍 Telegram bot configuration test message"
        message_response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message_text}
        )
        
        if message_response.status_code != 200:
            logger.error(f"Failed to send message. Response: {message_response.text}")
            if "chat not found" in message_response.text.lower():
                logger.error("The chat ID may be incorrect or the bot is not a member of the chat")
            return False
        
        logger.info("✅ Test message sent successfully!")
        return True
    
    except Exception as e:
        logger.error(f"Error testing Telegram bot: {e}")
        return False

if __name__ == "__main__":
    logger.info("Testing Telegram bot configuration...")
    test_telegram_bot()