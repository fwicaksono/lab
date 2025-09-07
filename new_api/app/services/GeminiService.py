from config.setting import env
from langchain_google_vertexai import ChatVertexAI, HarmBlockThreshold, HarmCategory
from config.credentials import google_credential

class GeminiService:
    def __init__(self):
        pass

    def gemini_20_flash(self):
        safety_settings = {
              HarmCategory.HARM_CATEGORY_UNSPECIFIED: HarmBlockThreshold.BLOCK_NONE,
              HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
              HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
              HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
              HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE
            }
        llm = ChatVertexAI(model_name=env.gemini_model, temperature=0, safety_settings=safety_settings, credentials=google_credential())
        return llm

gemini_service = GeminiService()