var socket = io('io:4242/ws');

var vm = new Vue({
  el: "#chat",
  data: {
    messages: [],
    input: ""
  },
  methods: {
    post: function(e) {
      msg = this.input
      console.log('send', msg)
      socket.emit('chat message', msg);
      e.preventDefault();
    }
  }
});

socket.on('chat message', function(msg){
  vm.messages.push(msg);
});


