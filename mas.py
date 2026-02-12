"""
Multi-Agent System (MAS) RAG Agent for Austrian Companies and Projects
Uses DSPy to create an intelligent agent that can answer questions about companies and projects
"""

import os
from typing import List, Dict, Any
import pandas as pd
import dspy


# Global data storage
COMPANIES_DF = None
PROJECTS_DF = None


def load_companies(filepath: str = "data/companies.csv") -> pd.DataFrame:
    """Load companies data from CSV file using pandas"""
    df = pd.read_csv(filepath, encoding='utf-8')
    # Drop rows where name is null or empty
    df = df.dropna(subset=['name'])
    df = df[df['name'].str.strip() != '']
    return df


def load_projects(filepath: str = "data/projectfacts.csv") -> pd.DataFrame:
    """Load project facts data from CSV file using pandas"""
    df = pd.read_csv(filepath, encoding='utf-8')
    # Drop rows where name is null or empty
    df = df.dropna(subset=['name'])
    df = df[df['name'].str.strip() != '']
    return df


def initialize_data():
    """Initialize the data from CSV files"""
    global COMPANIES_DF, PROJECTS_DF
    COMPANIES_DF = load_companies()
    PROJECTS_DF = load_projects()
    print(f"Loaded {len(COMPANIES_DF)} companies and {len(PROJECTS_DF)} projects")


# Tool functions for the RAG agent
def search_companies_by_name(company_name: str) -> List[Dict[str, Any]]:
    """Search for companies by name (case-insensitive partial match)"""
    mask = COMPANIES_DF['name'].str.contains(company_name, case=False, na=False)
    results_df = COMPANIES_DF[mask]
    return results_df.to_dict('records')


def search_companies_by_branch(branch: str) -> List[Dict[str, Any]]:
    """Search for companies by branch/industry (case-insensitive partial match)"""
    mask = COMPANIES_DF['branch'].str.contains(branch, case=False, na=False)
    results_df = COMPANIES_DF[mask]
    return results_df.to_dict('records')


def search_companies_by_location(location: str) -> List[Dict[str, Any]]:
    """Search for companies by location/address (case-insensitive partial match)"""
    mask = COMPANIES_DF['address'].str.contains(location, case=False, na=False)
    results_df = COMPANIES_DF[mask]
    return results_df.to_dict('records')


def get_company_details(company_name: str) -> Dict[str, Any]:
    """Get detailed information about a specific company by exact or close name match"""
    # Try exact match first (case-insensitive)
    exact_match = COMPANIES_DF[COMPANIES_DF['name'].str.lower() == company_name.lower()]
    if not exact_match.empty:
        return exact_match.iloc[0].to_dict()
    
    # Try partial match
    partial_match = COMPANIES_DF[COMPANIES_DF['name'].str.contains(company_name, case=False, na=False)]
    if not partial_match.empty:
        return partial_match.iloc[0].to_dict()
    
    return {"error": f"Company '{company_name}' not found"}


def search_projects_by_name(project_name: str) -> List[Dict[str, Any]]:
    """Search for projects by name (case-insensitive partial match)"""
    mask = PROJECTS_DF['name'].str.contains(project_name, case=False, na=False)
    results_df = PROJECTS_DF[mask]
    return results_df.to_dict('records')


def search_projects_by_location(location: str) -> List[Dict[str, Any]]:
    """Search for projects by location/address (case-insensitive partial match)"""
    mask = PROJECTS_DF['address'].str.contains(location, case=False, na=False)
    results_df = PROJECTS_DF[mask]
    return results_df.to_dict('records')


def find_companies_by_project_location(project_name: str) -> List[Dict[str, Any]]:
    """Find companies that are located at the same address as a project"""
    # First find the project
    project_results = search_projects_by_name(project_name)
    if not project_results:
        return []
    
    project_address = project_results[0].get('address', '')
    if not project_address:
        return []
    
    # Find companies at the same location
    return search_companies_by_location(project_address)


def get_all_companies() -> List[Dict[str, Any]]:
    """Get all companies in the database"""
    return COMPANIES_DF.to_dict('records')


def get_all_projects() -> List[Dict[str, Any]]:
    """Get all projects in the database"""
    return PROJECTS_DF.to_dict('records')


def get_companies_by_size(min_headcount: int = 0, max_headcount: int = 10000) -> List[Dict[str, Any]]:
    """Get companies within a specific headcount range"""
    # Convert headcount to numeric, coercing errors to NaN
    headcount_numeric = pd.to_numeric(COMPANIES_DF['headcount'], errors='coerce')
    mask = (headcount_numeric >= min_headcount) & (headcount_numeric <= max_headcount)
    results_df = COMPANIES_DF[mask]
    return results_df.to_dict('records')


# DSPy Signature for the RAG Agent
class AustrianCompanyProjectAgent(dspy.Signature):
    """You are an intelligent assistant that helps users find information about Austrian companies and projects.
    
    You have access to a database of Austrian companies with information including:
    - Company names, headcount, branches/industries, revenue, addresses, descriptions, and recent news
    
    You also have access to project information including:
    - Project names and their locations
    
    You can search by company name, branch/industry, location, project name, and find relationships between
    companies and projects based on location.
    
    Use the available tools to answer user questions accurately and comprehensively."""
    
    user_question: str = dspy.InputField(desc="The user's question about companies or projects")
    answer: str = dspy.OutputField(
        desc="A comprehensive answer to the user's question based on the retrieved information"
    )


# Create the RAG Agent
class CompanyProjectRAGAgent:
    """RAG Agent for querying Austrian companies and projects"""
    
    def __init__(self, model: str = "openai/gpt-4o-mini"):
        """Initialize the RAG agent with a language model"""
        # Initialize data
        initialize_data()
        
        # Configure DSPy
        lm = dspy.LM(model)
        dspy.configure(lm=lm)
        
        # Create ReAct agent with all tools
        self.agent = dspy.ReAct(
            AustrianCompanyProjectAgent,
            tools=[
                search_companies_by_name,
                search_companies_by_branch,
                search_companies_by_location,
                get_company_details,
                search_projects_by_name,
                search_projects_by_location,
                find_companies_by_project_location,
                get_all_companies,
                get_all_projects,
                get_companies_by_size,
            ]
        )
    
    def query(self, question: str) -> str:
        """Ask a question and get an answer from the RAG agent"""
        result = self.agent(user_question=question)
        return result.answer
    
    def query_with_details(self, question: str) -> dspy.Prediction:
        """Ask a question and get the full prediction object with trajectory"""
        return self.agent(user_question=question)


# Example usage and CLI interface
def main():
    """Main function to run the RAG agent interactively"""
    import sys
    
    # Check if OpenAI API key is set
    if not os.environ.get("OPENAI_API_KEY"):
        print("WARNING: OPENAI_API_KEY environment variable not set!")
        print("Please set it with: export OPENAI_API_KEY='your-key-here'")
        print("Or set it in your script before running.")
        sys.exit(1)
    
    print("=" * 70)
    print("Austrian Companies & Projects RAG Agent (powered by DSPy)")
    print("=" * 70)
    
    # Initialize the agent
    print("\nInitializing agent...")
    agent = CompanyProjectRAGAgent()
    
    print("\n" + "=" * 70)
    print("Agent ready! You can ask questions about Austrian companies and projects.")
    print("=" * 70)
    
    # Example questions
    example_questions = [
        "What companies are working in robotics?",
        "Tell me about companies in Vienna",
        "What is the AUTARK Demonstrator project?",
        "Which companies are located in Innsbruck?",
        "What are the largest companies by headcount?",
        "Tell me about Meshmakers GmbH",
        "What companies work on AI and data analytics?",
    ]
    
    print("\nExample questions you can ask:")
    for i, q in enumerate(example_questions, 1):
        print(f"  {i}. {q}")
    
    print("\n" + "-" * 70)
    
    # Interactive loop
    while True:
        print("\nYour question (or 'quit' to exit, 'examples' to see examples):")
        question = input("> ").strip()
        
        if question.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break
        
        if question.lower() == 'examples':
            print("\nExample questions:")
            for i, q in enumerate(example_questions, 1):
                print(f"  {i}. {q}")
            continue
        
        if not question:
            continue
        
        print("\nThinking...")
        try:
            # Get detailed response
            result = agent.query_with_details(question)
            
            print("\n" + "=" * 70)
            print("ANSWER:")
            print("=" * 70)
            print(result.answer)
            
            # Optionally show reasoning
            if hasattr(result, 'reasoning'):
                print("\n" + "-" * 70)
                print("REASONING:")
                print("-" * 70)
                print(result.reasoning)
            
            print("\n" + "=" * 70)
        
        except Exception as e:
            print(f"\nError: {e}")
            print("Please try another question.")


if __name__ == "__main__":
    main()
