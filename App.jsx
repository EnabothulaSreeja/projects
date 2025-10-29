import { useState } from "react";
import axios from "axios";

function App() {
  const [query, setQuery] = useState("");
  const [books, setBooks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setError("");
    try {
      const res = await axios.get(
        `https://openlibrary.org/search.json?title=${encodeURIComponent(query)}`
      );
      setBooks(res.data.docs.slice(0, 20));
    } catch (err) {
      setError("Failed to fetch books. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-indigo-100 to-white p-6">
      <h1 className="text-4xl font-bold text-center text-indigo-600 mb-8">
        📚 Book Finder
      </h1>

      <form onSubmit={handleSearch} className="flex justify-center mb-6 gap-2">
        <input
          type="text"
          placeholder="Search for books..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="w-80 px-4 py-2 border rounded-lg shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-400"
        />
        <button
          type="submit"
          className="bg-indigo-600 text-white px-5 py-2 rounded-lg hover:bg-indigo-700 transition"
        >
          Search
        </button>
      </form>

      {loading && <p className="text-center text-gray-600">Loading...</p>}
      {error && <p className="text-center text-red-500">{error}</p>}

      <div className="grid gap-6 md:grid-cols-3 lg:grid-cols-4 sm:grid-cols-2">
        {books.map((book, index) => (
          <div
            key={index}
            className="bg-white p-4 rounded-xl shadow hover:shadow-lg transition"
          >
            <img
              src={
                book.cover_i
                  ? `https://covers.openlibrary.org/b/id/${book.cover_i}-M.jpg`
                  : "https://via.placeholder.com/150x200?text=No+Cover"
              }
              alt={book.title}
              className="w-full h-64 object-cover rounded-md mb-3"
            />
            <h3 className="text-lg font-semibold text-indigo-700">
              {book.title}
            </h3>
            <p className="text-gray-600 text-sm">
              {book.author_name ? book.author_name.join(", ") : "Unknown Author"}
            </p>
            <p className="text-gray-500 text-xs mt-1">
              First Published: {book.first_publish_year || "N/A"}
            </p>
          </div>
        ))}
      </div>

      {books.length === 0 && !loading && !error && (
        <p className="text-center text-gray-600 mt-10">
          Start by searching for your favorite books!
        </p>
      )}
    </div>
  );
}

export default App;
