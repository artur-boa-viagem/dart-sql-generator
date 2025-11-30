import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from endpoints.init import api_router

app = FastAPI(
    title="Projeto TAES - SQL Generator",
    version="1.0.0",
    swagger_ui_parameters={"displayRequestDuration": True},
)

# Enable CORS for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (for development)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@app.get("/health")
def health_check():
    return {"status": "healthy"}

app.include_router(api_router, prefix="/api/v1")


if __name__ == "__main__":
    uvicorn.run("endpoints.server:app", host="0.0.0.0", port=8000, reload=True)
