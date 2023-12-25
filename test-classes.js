class Animal {
  speak() {
    return this;
  }
  static eat() {
    return this;
  }
}

const obj = new Animal();
console.log(obj.speak()); // the Animal object
const speak = obj.speak;
console.log(speak()); // undefined

console.log(Animal.eat()); // class Animal
const eat = Animal.eat;
console.log(eat()); // undefined
