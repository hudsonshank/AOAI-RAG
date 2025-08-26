# 🤖 AOAI-RAG - Advanced Azure OpenAI RAG System

A sophisticated Retrieval-Augmented Generation (RAG) system built on Azure OpenAI and Azure AI Search, designed to provide intelligent document search and conversational AI capabilities for enterprise knowledge management.

## Features

- **Advanced RAG Engine**: Multi-step reasoning with intelligent query optimization
- **Hybrid Search**: Combines text and vector search with semantic ranking
- **Multi-turn Conversations**: Context-aware chat with conversation memory
- **Smart Citations**: Transparent source attribution with confidence scores
- **Enhanced Metadata**: Rich document categorization and filtering
- **Streaming Responses**: Real-time chat experience
- **PII Detection**: Automatic sensitive information filtering
- **Scalable Architecture**: From development to enterprise deployment

## Current Status

**Phase 1: Foundation** *In Progress*
- [x] Repository structure
- [x] Basic configuration
- [ ] Simple RAG engine with existing index
- [ ] Basic API endpoints
- [ ] Frontend integration

**Phase 2: Enhanced Features** 🔄 *Coming Next*
- [ ] Advanced query optimization
- [ ] Enhanced document processing
- [ ] Rich metadata extraction
- [ ] Improved chunking strategies

**Phase 3: Production Ready** 📋 *Planned*
- [ ] Comprehensive testing
- [ ] Performance optimization
- [ ] Deployment automation
- [ ] Monitoring and logging

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Flask API     │    │   Azure AI      │
│   (HTML/JS)     │◄──►│   (Python)      │◄──►│   Search        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │                        │
                       ┌─────────────────┐    ┌─────────────────┐
                       │   RAG Engine    │    │   Azure OpenAI  │
                       │   - Search      │◄──►│   - GPT-4       │
                       │   - Chat        │    │   - Embeddings  │
                       │   - Processing  │    └─────────────────┘
                       └─────────────────┘
```

## Quick Start

### Prerequisites
- Python 3.9+
- Azure subscription with:
  - Azure AI Search service
  - Azure OpenAI service
  - Azure Storage account (optional)

### 1. Setup Repository
```bash
git clone https://github.com/your-username/AOAI-RAG.git
cd AOAI-RAG

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your Azure credentials
# Required: AZURE_SEARCH_ADMIN_KEY, AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT
```

### 3. Test Basic Functionality
```bash
# Test your existing search index and Azure OpenAI integration
python scripts/test_basic_rag.py
```

### 4. Run Development Server
```bash
# Start the Flask API server
python src/api/app.py

# API will be available at http://localhost:5000
```

## Current Index Status

**Existing Index**: `jennifur-rag`
- 9,404+ documents indexed
- Vector search enabled (1536 dimensions)
- Semantic search configured
- Ready for advanced RAG

**Index Fields Available**:
- `chunk` - Document content
- `text_vector` - Embeddings (ada-002)
- `filename` - Source file names
- `document_path` - File paths
- `title` - Document titles
- Additional metadata fields

## 🛠️ Development Workflow

### Current Phase: Basic RAG Testing
```bash
# 1. Test configuration
python scripts/test_basic_rag.py

# 2. Test search functionality
python -c "from src.core.search_engine import SimpleSearch; print('Search test')"

# 3. Test chat functionality  
python -c "from src.core.chat_engine import SimpleChat; print('Chat test')"
```

### Next Phase: Enhanced Features
```bash
# Create enhanced index (when ready)
python scripts/setup_enhanced_index.py

# Migrate documents with enhanced processing
python scripts/migrate_documents.py

# Run comprehensive tests
pytest tests/
```

## 📁 Project Structure

```
AOAI-RAG/
├── src/                    # Source code
│   ├── core/              # RAG engine core
│   ├── api/               # Flask API
│   ├── config/            # Configuration
│   └── utils/             # Utilities
├── scripts/               # Utility scripts
├── tests/                 # Test suite
├── frontend/              # HTML/JS frontend
├── docs/                  # Documentation
└── azure-function/        # Integration with existing function
```

## Documentation

- [Setup Guide](docs/setup.md) - Detailed installation instructions
- [API Documentation](docs/api.md) - Complete API reference
- [Architecture Guide](docs/architecture.md) - System design and components
- [Deployment Guide](docs/deployment.md) - Production deployment
- [Troubleshooting](docs/troubleshooting.md) - Common issues and solutions

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src tests/

# Run specific test categories
pytest tests/unit/          # Unit tests
pytest tests/integration/   # Integration tests
```

## Performance Targets

- **Search Latency**: <200ms for typical queries
- **Chat Response**: <2s for complex questions  
- **Throughput**: 100+ concurrent users
- **Accuracy**: 90%+ citation accuracy

## Security Features

- Environment-based configuration
- PII detection and filtering
- CORS protection
- Input validation and sanitization
- Secure Azure service integration

## Contributing

1. Create a feature branch (`git checkout -b feature/your-feature`)
2. Make your changes
3. Add tests for new functionality
4. Run tests (`pytest`)
5. Commit your changes (`git commit -m 'Add your feature'`)
6. Push to the branch (`git push origin feature/your-feature`)
7. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- Check the [documentation](docs/)
- Report issues on [GitHub Issues](https://github.com/your-username/AOAI-RAG/issues)
- Join discussions in [GitHub Discussions](https://github.com/your-username/AOAI-RAG/discussions)

---

**Ready to get started?** 

1. Copy `.env.example` to `.env` and add your Azure credentials
2. Run `python scripts/test_basic_rag.py` to test your existing setup
3. Follow the [Setup Guide](docs/setup.md) for detailed instructions

**Current Status**: Testing basic functionality with existing 9,404 documents
