var keyword = document.getElementsByClassName("keyword");
var i;

for(i = 0; i < keyword.length; i++){
    if (keyword[i].firstElementChild.innerHTML == ""){
        keyword[i].addEventListener("mouseenter", function(){
            var description = "This section has already explained in other parts or yet to be implemented"
            var textbox = document.getElementById("annotation");
            textbox.innerHTML = description
        });

        keyword[i].addEventListener("mouseleave", function(){
            var description = "Hover over to see annotation!"
            var textbox = document.getElementById("annotation");
            textbox.innerHTML = description
        });
    }
    else{
        keyword[i].addEventListener("mouseenter", function(){
            var description = this.firstElementChild.innerHTML;
            var textbox = document.getElementById("annotation");
            textbox.innerHTML = description
        });
        keyword[i].addEventListener("mouseleave", function(){
            var description = "Hover over to see annotation!"
            var textbox = document.getElementById("annotation");
            textbox.innerHTML = description
        });
        keyword[i].classList.add("has-annotation")
    }
}