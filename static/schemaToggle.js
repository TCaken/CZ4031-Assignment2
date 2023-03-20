var toggler = document.getElementsByClassName("tree")
var i;

for(i = 0; i < toggler.length; i++){
    toggler[i].addEventListener("click", function(){
        if (this.classList.contains("database")){
            this.classList.toggle("tree-toggle");
        }
        this.parentElement.querySelector(".tree-node").classList.toggle("tree-node-active");
    });
}