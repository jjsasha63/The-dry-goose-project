// Check current path
console.log(window.location.href);
console.log(window.location.pathname);

// Test your regex
const match = window.location.pathname.match(/^(.*?)\/proxy\/(\d+)/);
console.log('match:', match);
console.log('basename result:', match ? match[1] : 'NO MATCH');
