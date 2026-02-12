"""
Demo script for the Multi-Agent System (MAS) RAG Agent
Shows example usage of the agent
"""

import os
from mas import CompanyProjectRAGAgent

# Make sure to set your OpenAI API key
# os.environ["OPENAI_API_KEY"] = "your-key-here"

def run_examples():
    """Run some example queries"""
    
    print("Initializing the RAG agent...")
    agent = CompanyProjectRAGAgent()
    
    # Example questions
    questions = [
        "What companies are working in robotics in Austria?",
        "Tell me about Meshmakers GmbH",
        "Which companies are located in Vienna?",
        "What is the AUTARK Demonstrator project and which companies might be involved?",
        "What are the top 3 largest companies by headcount?",
    ]
    
    print("\n" + "="*70)
    print("Running Example Queries")
    print("="*70)
    
    for i, question in enumerate(questions, 1):
        print(f"\n{'='*70}")
        print(f"Question {i}: {question}")
        print("="*70)
        
        try:
            # Get answer with full details
            result = agent.query_with_details(question)
            
            print("\nANSWER:")
            print(result.answer)
            
            if hasattr(result, 'reasoning'):
                print("\nREASONING:")
                print(result.reasoning)
            
            print("\n" + "-"*70)
        
        except Exception as e:
            print(f"Error: {e}")
    
    print("\n" + "="*70)
    print("Demo completed!")
    print("="*70)


if __name__ == "__main__":
    run_examples()
