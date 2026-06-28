<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Legend Family Chat</title>

<style>
body{
    margin:0;
    font-family:Arial;
    background:linear-gradient(180deg,#020814,#001a33,#003366);
    color:white;
}

header{
    text-align:center;
    padding:20px;
    background:rgba(0,0,0,.6);
    border-bottom:2px solid #00aaff;
}

#chat{
    height:70vh;
    overflow-y:auto;
    padding:15px;
}

.msg{
    background:#0b2545;
    padding:10px;
    margin-bottom:10px;
    border-radius:10px;
    border-left:4px solid #00aaff;
}

.bottom{
    position:fixed;
    bottom:0;
    left:0;
    right:0;
    display:flex;
    gap:10px;
    padding:10px;
    background:#001122;
}

input{
    padding:12px;
    border:none;
    border-radius:8px;
    outline:none;
}

#name{ width:120px; }
#message{ flex:1; }

button{
    padding:12px 15px;
    border:none;
    border-radius:8px;
    background:#00aaff;
    color:white;
    font-weight:bold;
}
</style>
</head>

<body>

<header>
    <h1>💙 Legend Family Chat 💙</h1>
</header>

<div id="chat"></div>

<div class="bottom">
    <input id="name" placeholder="Name">
    <input id="message" placeholder="Message">
    <button onclick="sendMessage()">Send</button>
</div>

<!-- FIREBASE -->
<script src="https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js"></script>
<script src="https://www.gstatic.com/firebasejs/10.12.0/firebase-database.js"></script>

<script>
// 🔥 PUT YOUR FIREBASE CONFIG HERE
const firebaseConfig = {
    apiKey: "YOUR_API_KEY",
    authDomain: "YOUR_PROJECT.firebaseapp.com",
    databaseURL: "YOUR_DATABASE_URL",
    projectId: "YOUR_PROJECT_ID",
};

firebase.initializeApp(firebaseConfig);
const db = firebase.database().ref("chat");

// SEND MESSAGE
function sendMessage(){
    const name = document.getElementById("name").value || "Guest";
    const message = document.getElementById("message").value;

    if(!message.trim()) return;

    db.push({
        name:name,
        message:message,
        time:Date.now()
    });

    document.getElementById("message").value="";
}

// RECEIVE MESSAGES (LIVE)
db.on("child_added", function(snapshot){
    const data = snapshot.val();

    const div = document.createElement("div");
    div.className = "msg";
    div.innerHTML = "<b>" + data.name + "</b><br>" + data.message;

    document.getElementById("chat").appendChild(div);

    document.getElementById("chat").scrollTop =
        document.getElementById("chat").scrollHeight;
});
</script>

</body>
</html>
