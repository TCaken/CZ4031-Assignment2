var query = document.getElementsByClassName("query-dropdown");
var i;

for(i = 0; i < query.length; i++){
    query[i].addEventListener("click", function(){
        var textarea = document.getElementById("query")
        textarea.value = "";
        textarea.value = this.firstElementChild.innerText;  
    }, false);
    console.log(query[i].firstElementChild.innerText);
}